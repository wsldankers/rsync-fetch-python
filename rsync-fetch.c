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
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
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

#ifdef WORDS_BIGENDIAN

#ifdef HAVE_BUILTIN_BSWAP16
#define le16(x) ((uint16_t)__builtin_bswap16(x))
#else
__attribute__((const,optimize(3)))
static inline uint16_t le16(uint16_t x) {
	return (x << 8) | (x >> 8);
}
#endif

#ifdef HAVE_BUILTIN_BSWAP32
#define le32(x) ((uint32_t)__builtin_bswap32(x))
#else
__attribute__((const,optimize(3)))
static inline uint32_t le32(uint32_t x) {
	x = ((x & UINT32_C(0x00FF00FF)) << 8) | ((x & UINT32_C(0xFF00FF00)) >> 8);
	return (x << 16) | (x >> 16);
}
#endif

#ifdef HAVE_BUILTIN_BSWAP64
#define le64(x) ((uint64_t)__builtin_bswap64(x))
#else
__attribute__((const,optimize(3)))
static inline uint64_t le64(uint64_t x) {
	x = ((x & UINT64_C(0x00FF00FF00FF00FF)) << 8) | ((x & UINT64_C(0xFF00FF00FF00FF00)) >> 8);
	x = ((x & UINT64_C(0x0000FFFF0000FFFF)) << 16) | ((x & UINT64_C(0xFFFF0000FFFF0000)) >> 16);
	return (x << 32) | (x >> 32);
}
#endif

#else

#define le16(x) (x)
#define le32(x) (x)
#define le64(x) (x)

#endif

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

enum message_id {
	MSG_DATA = 0,
	MSG_ERROR_XFER = 1,
	MSG_INFO = 2,
	MSG_ERROR = 3,
	MSG_WARNING = 4,
	MSG_ERROR_SOCKET = 5,
	MSG_LOG = 6,
	MSG_CLIENT = 7,
	MSG_ERROR_UTF8 = 8,
	MSG_REDO = 9,
	MSG_FLIST = 20,
	MSG_FLIST_EOF = 21,
	MSG_IO_ERROR = 22,
	MSG_NOOP = 42,
	MSG_DONE = 86,
	MSG_SUCCESS = 100,
	MSG_DELETED = 101,
	MSG_NO_SEND = 102,
};

typedef struct RsyncFetch {
	PyObject_HEAD
	uint64_t magic;
	pipestream_t stream[3];
	size_t multiplex_remaining;
	int pid;
	bool multiplex;
} RsyncFetch_t;

static struct PyModuleDef rsync_fetch_module;
static PyTypeObject RsyncFetch_type;

#define RSYNCFETCH_MAGIC UINT64_C(0x6FB32179D3F495D0)

static inline RsyncFetch_t *RsyncFetch_Check(PyObject *v) {
	return v && PyObject_TypeCheck(v, &RsyncFetch_type) && ((RsyncFetch_t *)v)->magic == RSYNCFETCH_MAGIC
		? (RsyncFetch_t *)v
		: NULL;
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
					PyErr_SetFromErrno(PyExc_OSError);
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
									rf->pid = pid;
									rf->stream[STREAM_IN] = pipestream_0;
									rf->stream[STREAM_OUT] = pipestream_0;
									rf->stream[STREAM_ERR] = pipestream_0;
									rf->stream[STREAM_IN].fd = in_pipe[0];
									rf->stream[STREAM_OUT].fd = out_pipe[1];
									rf->stream[STREAM_ERR].fd = err_pipe[0];
									rf->multiplex_remaining = 0;

									return &rf->ob_base;
								}
							}
						}
					}
					while(waitpid(pid, NULL, 0) == -1 && errno == EINTR);
				} else {
					if(dup2(out_pipe[0], STDIN_FILENO) == -1
					|| dup2(in_pipe[1], STDOUT_FILENO) == -1
					|| dup2(err_pipe[1], STDERR_FILENO) == -1) {
						perror("dup2");
						_exit(2);
					}

					if(fcntl(STDIN_FILENO, F_SETFL, 0) == -1
					|| fcntl(STDOUT_FILENO, F_SETFL, 0) == -1
					|| fcntl(STDERR_FILENO, F_SETFL, 0) == -1) {
						perror("fcntl");
						_exit(2);
					}

#ifdef SIGPIPE
					signal(SIGPIPE, SIG_DFL);
#endif
#ifdef SIGXFZ
					signal(SIGXFZ, SIG_DFL);
#endif
#ifdef SIGXFSZ
					signal(SIGXFSZ, SIG_DFL);
#endif

					char * const argv[] = { "cat", NULL };
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
		for(int i = 0; i < STREAM_NUM; i++) {
			pipestream_t *stream = &rf->stream[i];
			if(stream->fd != -1)
				close(stream->fd);
			bucket_t *head = stream->buf_head; 
			while(head) {
				bucket_t *next = head->next;
				free(head);
				head = next;
			}
		}
		if(rf->pid)
			while(waitpid(rf->pid, NULL, 0) == -1 && errno == EINTR);
	}

	freefunc tp_free = Py_TYPE(self)->tp_free ?: PyObject_Free;
	tp_free(self);

	return 0;
}

static bool stream_read(RsyncFetch_t *rf, pipestream_t *stream) {
	bucket_t *tail = stream->buf_tail;
	size_t avail;
	size_t end_offset = stream->end_offset;
	bucket_t *prev;
	if(tail) {
		prev = tail->prev;
		if(!prev) {
			size_t start_offset = stream->start_offset;
			size_t used = end_offset - start_offset;
			if(used <= 256) {
				memmove(tail->buf, tail->buf + start_offset, used);
				stream->start_offset = 0;
				stream->end_offset = used;
				end_offset = used;
			}
		}

		avail = sizeof tail->buf - end_offset;
		if(!avail) {
			prev = tail;
			tail = malloc(sizeof *tail);
			if(!tail)
				return false;
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
		tail = malloc(sizeof *tail);
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

static bool stream_write(RsyncFetch_t *rf, pipestream_t *stream) {
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
		stream->start_offset = start_offset + r;
	}
	return true;
}

static bool do_io(RsyncFetch_t *rf) {
	struct pollfd pfds[3];
	for(int i = 0; i < STREAM_NUM; i++) {
		pipestream_t *stream = &rf->stream[i];
		if(i == STREAM_OUT) {
			pfds[i].fd = stream->length ? stream->fd : -1;
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
				pipestream_t *stream = &rf->stream[i];
				close(stream->fd);
				stream->fd = -1;
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
		if(pfds[i].revents & POLLIN)
			return stream_read(rf, &rf->stream[i]);
	}

	return true;
}

static bool wait_for_eof(RsyncFetch_t *rf) {
	for(;;) {
		if(rf->stream[STREAM_IN].length) {
			errno = EBUSY;
			return false;
		}

		struct pollfd pfds[3];
		for(int i = 0; i < STREAM_NUM; i++) {
			pipestream_t *stream = &rf->stream[i];
			if(i == STREAM_OUT) {
				pfds[i].fd = stream->length ? stream->fd : -1;
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
			pipestream_t *stream = &rf->stream[i];
			if(pfds[i].revents & POLLIN)
				if(!stream_read(rf, stream))
					return false;
			if(pfds[i].revents & POLLOUT)
				if(!stream_write(rf, stream))
					return false;
			if(pfds[i].revents & (POLLERR|POLLHUP)) {
				close(stream->fd);
				stream->fd = -1;
				if(i == STREAM_IN)
					return true;
			}
		}
	}
}

static bool stream_enqueue(RsyncFetch_t *rf, pipestream_t *stream, const char *buf, size_t len) {
	bucket_t *tail = stream->buf_tail;
	if(tail) {
		size_t end_offset = stream->end_offset;
		if(!tail->prev) {
			size_t start_offset = stream->start_offset;
			size_t used = end_offset - start_offset;
			if(used <= 256) {
				memmove(tail->buf, tail->buf + start_offset, used);
				stream->start_offset = 0;
				stream->end_offset = used;
				end_offset = used;
			}
		}
		size_t avail = sizeof tail->buf - end_offset;
		if(avail) {
			if(len > avail) {
				memcpy(tail->buf + end_offset, buf, avail);
				len -= avail;
				buf += avail;
				stream->end_offset = sizeof tail->buf;
				stream->length += avail;
			} else {
				memcpy(tail->buf + end_offset, buf, len);
				stream->end_offset = end_offset + len;
				stream->length += len;
				return true;
			}
		}
	}

	for(;;) {
		bucket_t *prev = tail;
		tail = malloc(sizeof *tail);
		if(!tail)
			return false;
		tail->prev = prev;
		tail->next = NULL;
		if(prev)
			prev->next = tail;
		else
			stream->buf_head = tail;
		stream->buf_tail = tail;

		if(len > sizeof tail->buf) {
			memcpy(tail->buf, buf, sizeof tail->buf);
			buf += sizeof tail->buf;
			len -= sizeof tail->buf;
			stream->end_offset = sizeof tail->buf;
			stream->length += sizeof tail->buf;
		} else {
			memcpy(tail->buf, buf, len);
			stream->end_offset = len;
			stream->length += len;
			return true;
		}
	}
}

static bool stream_dequeue(RsyncFetch_t *rf, pipestream_t *stream, char *buf, size_t len) {
	if(!len)
		return true;
	while(stream->length < len)
		if(!do_io(rf))
			return false;
	for(;;) {
		bucket_t *head = stream->buf_head;
		bucket_t *next = head->next;
		size_t start_offset = stream->start_offset;
		size_t avail = (next ? sizeof head->buf : stream->end_offset) - start_offset;
		if(avail > len) {
			memcpy(buf, head->buf + start_offset, len);
			stream->start_offset = start_offset + len;
			stream->length -= len;
			return true;
		} else {
			memcpy(buf, head->buf + start_offset, avail);
			stream->length -= avail;
			stream->start_offset = 0;
			if(next) {
				free(head);
				head = next;
				len -= avail;
				buf += avail;
				start_offset = 0;
				next->prev = NULL;
				stream->buf_head = next;
			} else {
				assert(len == avail);
				stream->end_offset = 0;
				return true;
			}
		}
	}
}

static bool stream_multiplex(RsyncFetch_t *rf, pipestream_t *stream, char *buf, size_t len) {
	if(!rf->multiplex)
		return stream_enqueue(rf, stream, buf, len);
	while(len) {
		size_t chunk = len < 0xFFFFFF ? len : 0xFFFFFF;
		uint8_t mplex[4] = { chunk, chunk >> 8, chunk >> 16, MSG_DATA };
		if(!stream_enqueue(rf, stream, (char *)mplex, sizeof mplex))
			return false;
		if(!stream_enqueue(rf, stream, buf, chunk))
			return false;
		len -= chunk;
		buf += chunk;
	}
	return true;
}

static bool stream_demultiplex(RsyncFetch_t *rf, pipestream_t *stream, char *buf, size_t len) {
	if(!rf->multiplex)
		return stream_dequeue(rf, stream, buf, len);
	size_t multiplex_remaining = rf->multiplex_remaining;
	for(;;) {
		if(multiplex_remaining < len) {
			if(multiplex_remaining) {
				if(!stream_dequeue(rf, stream, buf, multiplex_remaining))
					return false;
				buf += multiplex_remaining;
				len -= multiplex_remaining;
			}
		} else {
			rf->multiplex_remaining = multiplex_remaining - len;
			return stream_dequeue(rf, stream, buf, len);
		}

		for(;;) {
			uint8_t mplex[4];
			if(!stream_dequeue(rf, stream, (char *)mplex, sizeof mplex))
				return false;
			uint8_t channel = mplex[3];
			size_t multiplex_remaining = mplex[0] | mplex[1] << 8 | mplex[2] << 16;

			if(channel == MSG_DATA)
				break;

			if(multiplex_remaining) {
				char *message = malloc(multiplex_remaining);
				if(!message)
					return false;
				if(!stream_dequeue(rf, stream, message, multiplex_remaining)) {
					free(message);
					return false;
				}
				// FIXME: handle message
				free(message);
			} else {
				// FIXME: handle empty message
			}
		}
	}
}

static bool stream_flush(RsyncFetch_t *rf, pipestream_t *stream) {
	while(stream->length)
		if(!do_io(rf))
			return false;
	return true;
}

static bool send_int8(RsyncFetch_t *rf, int8_t d) {
	return stream_multiplex(rf, &rf->stream[STREAM_OUT], (char *)&d, sizeof d);
}

static bool send_uint8(RsyncFetch_t *rf, uint8_t d) {
	return stream_multiplex(rf, &rf->stream[STREAM_OUT], (char *)&d, sizeof d);
}

static bool send_int16(RsyncFetch_t *rf, int16_t d) {
	int16_t le = le16(d);
	return stream_multiplex(rf, &rf->stream[STREAM_OUT], (char *)&le, sizeof le);
}

static bool send_uint16(RsyncFetch_t *rf, uint16_t d) {
	uint16_t le = le16(d);
	return stream_multiplex(rf, &rf->stream[STREAM_OUT], (char *)&le, sizeof le);
}

static bool send_int32(RsyncFetch_t *rf, int32_t d) {
	int32_t le = le32(d);
	return stream_multiplex(rf, &rf->stream[STREAM_OUT], (char *)&le, sizeof le);
}

static bool send_uint32(RsyncFetch_t *rf, uint32_t d) {
	uint32_t le = le32(d);
	return stream_multiplex(rf, &rf->stream[STREAM_OUT], (char *)&le, sizeof le);
}

static bool send_int64(RsyncFetch_t *rf, int64_t d) {
	int64_t le = le64(d);
	return stream_multiplex(rf, &rf->stream[STREAM_OUT], (char *)&le, sizeof le);
}

static bool send_uint64(RsyncFetch_t *rf, uint64_t d) {
	uint64_t le = le64(d);
	return stream_multiplex(rf, &rf->stream[STREAM_OUT], (char *)&le, sizeof le);
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

	PyObject *ret = PyBytes_FromStringAndSize(NULL, howmany);
	if(!ret)
		return NULL;

	if(stream_demultiplex(rf, &rf->stream[STREAM_IN], PyBytes_AsString(ret), howmany))
		return ret;

	Py_DecRef(ret);
	return PyErr_SetFromErrno(PyExc_OSError);
}

static PyObject *RsyncFetch_writebytes(PyObject *self, PyObject *bytes) {
	RsyncFetch_t *rf = RsyncFetch_Check(self);
	if(!rf)
		return NULL;

	char *buf;
	Py_ssize_t len;
	if(PyBytes_AsStringAndSize(bytes, &buf, &len) == -1)
		return NULL;

//fprintf(stderr, "%s:%d: len=%zd\n", __FILE__, __LINE__, len);

	if(stream_multiplex(rf, &rf->stream[STREAM_OUT], buf, len))
		Py_RETURN_NONE;

	return PyErr_SetFromErrno(PyExc_OSError);
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
	{"writebytes", RsyncFetch_writebytes, METH_O, "write some bytes"},
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

