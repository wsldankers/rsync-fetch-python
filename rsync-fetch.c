#define _LARGEFILE64_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <errno.h>
#include <poll.h>
#include <sys/uio.h>

#define PY_SSIZE_T_CLEAN

#include "Python.h"
#include "pythread.h"

#ifdef WITH_THREAD
#define DECLARE_THREAD_SAVE PyThreadState *_save;
#else
#define DECLARE_THREAD_SAVE
#endif

#define orz(x) (sizeof (x) / sizeof *(x))

typedef struct bucket {
	struct bucket *next;
	struct bucket *prev;
	char buf[65536];
} bucket_t;

typedef struct pipestream {
	int fd;
	bucket_t *buf_head;
	bucket_t *buf_tail;
	size_t length; // total amount of data in this stream
	size_t end_offset; // where does the unused buffer space in the last bucket start
	size_t start_offset; // where does the remaining data in the first bucket start
} pipestream_t;
static const pipestream_t pipestream_0 = {.fd = -1};

enum {
	STREAM_IN,
	STREAM_OUT,
	STREAM_ERR,
	STREAM_NUM
};

typedef struct RsyncFetch {
	PyObject_HEAD
	uint64_t magic;
	pipestream_t stream[3];
	int pid;
} RsyncFetch_t;

static struct PyModuleDef rsync_fetch_module;
static PyTypeObject RsyncFetch_type;

#define RSYNCFETCH_MAGIC UINT64_C(0x6FB32179D3F495D0)

static inline RsyncFetch_t *RsyncFetch_Check(PyObject *v) {
	return v && PyObject_TypeCheck(v, &RsyncFetch_type) && ((RsyncFetch_t *)v)->magic == RSYNCFETCH_MAGIC
		? (RsyncFetch_t *)v
		: NULL;
}

static bool stream_read(RsyncFetch_t *tf, pipestream_t *stream) {
	bucket_t *tail = stream->buf_tail;
	size_t avail;
	size_t end_offset = stream->end_offset;
	bucket_t *prev;
	if(tail) {
		prev = tail->prev;
		avail = sizeof tail->buf - end_offset;
		if(!avail) {
			prev = tail;
			tail = malloc(sizeof tail);
			tail->prev = prev;
			tail->next = NULL;
			prev->next = tail;
			stream->buf_tail = tail;
			stream->end_offset = 0;
			end_offset = 0;
			avail = sizeof tail->buf;
		}
	} else {
		prev = NULL;
		tail = malloc(sizeof tail);
		if(!tail)
			return false;
		tail->prev = NULL;
		tail->next = NULL;
		stream->buf_head = tail;
		stream->buf_tail = tail;
		avail = sizeof tail->buf;
	}
	
	ssize_t r = read(stream->fd, tail->buf + end_offset, avail);
	if(r == -1)
		return false;

	stream->end_offset = end_offset + r;
	stream->length += r;

	return true;
}

static bool stream_write(RsyncFetch_t *tf, pipestream_t *stream) {
	bucket_t *head = stream->buf_head;
	assert(head);
	bucket_t *next = head->next;
	size_t start_offset = stream->start_offset;
	size_t head_usage = (next ? sizeof head->buf : stream->end_offset) - start_offset;
	size_t r = write(stream->fd, head->buf + start_offset, head_usage);
	if(r == -1)
		return false;
	stream->length -= r;
	if(r == head_usage) {
		stream->start_offset = 0;
		if(next) {
			free(head);
			stream->buf_head = next;
			next->prev = NULL;
		} else {
			stream->end_offset = 0;
		}
	} else {
		start_offset += r;
		stream->start_offset = start_offset;
		if(!next) {
			head_usage -= r;
			if(head_usage <= 256) {
				memmove(head->buf, head->buf + start_offset, head_usage);
				stream->start_offset = 0;
				stream->end_offset = head_usage;
			}
		}
	}
	return true;
}

#ifndef HAVE_PIPE2
static int pipe2(int *fds, int flags) {
	if(pipe(fds) == -1)
		return -1;
	if(fcntl(fds[0], F_SETFD, flags) == -1 || fcntl(fds[1], F_SETFD, flags) == -1) {
		int *errno_pointer = &errno;
		int saved_errno = *errno_pointer;
		close(fds[0]);
		close(fds[1]);
		*errno_pointer = saved_errno;
		return -1;
	}
	return 0;
}
#endif

static int create_pipe(int *fds) {
	if(pipe2(fds, O_CLOEXEC|O_NONBLOCK) == -1)
		return -1;
	if(fds[0] < 3 || fds[1] < 3) {
		close(fds[0]);
		close(fds[1]);
		errno = EBADFD;
		return -1;
	}
	return 0;
}

static PyObject *RsyncFetch_new(PyTypeObject *subtype, PyObject *args, PyObject *kwargs) {
	int in_pipe[2], out_pipe[2], err_pipe[2];
	if(create_pipe(in_pipe) == -1) {
		PyErr_SetFromErrno(PyExc_OSError);
	} else {
		if(create_pipe(out_pipe) == -1) {
			PyErr_SetFromErrno(PyExc_OSError);
		} else {
			if(create_pipe(err_pipe) == -1) {
				PyErr_SetFromErrno(PyExc_OSError);
			} else {
				int pid = vfork();
				if(pid == -1) {
					return PyErr_SetFromErrno(PyExc_OSError);
				} else if(pid) {
					if(close(in_pipe[1]) == -1) {
						PyErr_SetFromErrno(PyExc_OSError);
					} else {
						in_pipe[1] = -1;
						if(close(out_pipe[0]) == -1) {
							PyErr_SetFromErrno(PyExc_OSError);
						} else {
							out_pipe[0] = -1;
							if(close(err_pipe[1]) == -1) {
								PyErr_SetFromErrno(PyExc_OSError);
							} else {
								err_pipe[1] = -1;

								RsyncFetch_t *rf;
								rf = PyObject_New(RsyncFetch_t, subtype);
								if(rf) {
									rf->magic = RSYNCFETCH_MAGIC;
									rf->stream[STREAM_IN] = pipestream_0;
									rf->stream[STREAM_OUT] = pipestream_0;
									rf->stream[STREAM_ERR] = pipestream_0;
									rf->pid = 0;

									rf->stream[STREAM_IN].fd = in_pipe[0];
									rf->stream[STREAM_OUT].fd = out_pipe[1];
									rf->stream[STREAM_ERR].fd = err_pipe[0];

									return &rf->ob_base;
								}
							}
						}
					}
				} else {
					if(dup2(out_pipe[0], STDIN_FILENO) == -1
					|| dup2(in_pipe[1], STDOUT_FILENO) == -1
					|| dup2(err_pipe[1], STDERR_FILENO) == -1) {
						perror("dup2");
						_exit(2);
					}

					char * const argv[] = { "sleep", "1", NULL };
					execvp(argv[0], argv);
					perror("execvp");
					_exit(2);
				}
				if(err_pipe[0] != -1)
					close(err_pipe[0]);
				close(err_pipe[1]);
			}
			close(out_pipe[0]);
			if(out_pipe[1] != -1)
				close(out_pipe[1]);
		}
		if(in_pipe[0] != -1)
			close(in_pipe[0]);
		close(in_pipe[1]);
	}
	return NULL;
}

static int RsyncFetch_dealloc(PyObject *self) {
	RsyncFetch_t *rf = RsyncFetch_Check(self);
	if(rf) {
		rf->magic = 0;
		for(int i = 0; i < STREAM_NUM; i++)
			if(rf->stream[i].fd != -1)
				close(rf->stream[i].fd);
	}

	freefunc tp_free = Py_TYPE(self)->tp_free ?: PyObject_Free;
	tp_free(self);

	return 0;
}

static bool do_io(RsyncFetch_t *rf) {
	struct pollfd pfds[3];
	for(int i = 0; i < STREAM_NUM; i++) {
		pipestream_t *stream = &rf->stream[i];
		if(i == STREAM_OUT) {
			pfds[i].fd = stream->buf_head ? stream->fd : -1;
			pfds[i].events = POLLOUT;
		} else {
			pfds[i].fd = stream->fd;
			pfds[i].events = POLLIN;
		}
	}

	int r = poll(pfds, orz(pfds), 500);
	if(r == -1)
		return false;
	if(r == 0) {
		errno = ETIME;
		return false;
	}
	for(int i = 0; i < orz(pfds); i++) {
		if(pfds[i].revents & (POLLERR|POLLHUP)) {
			if(i == STREAM_ERR) {
				close(rf->stream[i].fd);
				rf->stream[i].fd = -1;
			} else {
				errno = EPIPE;
				return false;
			}
		}
	}
	for(int i = 0; i < orz(pfds); i++) {
		if(pfds[i].revents & POLLOUT)
			return stream_write(rf, &rf->stream[i]);
	}
	for(int i = 0; i < orz(pfds); i++) {
		if(pfds[i].revents & POLLIN) {
			return stream_read(rf, &rf->stream[i]);
		}
	}

	return true;
}

static PyObject *RsyncFetch_readbytes(PyObject *self, PyObject *howmany_obj) {
	PyErr_Clear();
	Py_ssize_t howmany = PyNumber_AsSsize_t(howmany_obj, PyExc_OverflowError);
	if(howmany == -1 && PyErr_Occurred())
		return NULL;
	if(howmany < 0)
		return PyErr_Format(PyExc_ValueError, "cannot read a negative number of bytes");

	RsyncFetch_t *rf = RsyncFetch_Check(self);
	if(!rf)
		return NULL;

	while(rf->stream[STREAM_IN].length < howmany)
		if(!do_io(rf))
			return PyErr_SetFromErrno(PyExc_OSError);
		
	return PyErr_Format(PyExc_NotImplementedError, "not implemented yet - teehee");
}

static PyObject *RsyncFetch_self(PyObject *self, PyObject *args) {
	Py_IncRef(self);
	return self;
}

static PyObject *RsyncFetch_none(PyObject *self, PyObject *args) {
	Py_RETURN_NONE;
}

static PyMethodDef RsyncFetch_methods[] = {
	{"__enter__", (PyCFunction)RsyncFetch_self, METH_NOARGS, "return a context manager for 'with'"},
	{"__exit__", RsyncFetch_none, METH_VARARGS, "callback for 'with' context manager"},
	{"readbytes", RsyncFetch_readbytes, METH_O, "read some bytes"},
	{NULL}
};

static PyTypeObject RsyncFetch_type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_basicsize = sizeof(RsyncFetch_t),
	.tp_name = "rsync_fetch.RsyncFetch",
	.tp_new = (newfunc)RsyncFetch_new,
	.tp_dealloc = (destructor)RsyncFetch_dealloc,
	.tp_methods = RsyncFetch_methods,
};

PyDoc_STRVAR(rsync_fetch_module_doc, "Minimal implementation of the rsync protocol for fetching files");

static struct PyModuleDef rsync_fetch_module = {
	PyModuleDef_HEAD_INIT,
	.m_name = "rsync_fetch",
	.m_doc = rsync_fetch_module_doc,
};

PyMODINIT_FUNC PyInit_rsync_fetch(void) {
	if(PyType_Ready(&RsyncFetch_type) == -1)
		return NULL;

	PyObject *module = PyModule_Create(&rsync_fetch_module);
	if(module) {
		if(PyModule_AddObject(module, "RsyncFetch", &RsyncFetch_type.ob_base.ob_base) != -1)
			return module;
		Py_DecRef(module);
	}
	return NULL;
}

