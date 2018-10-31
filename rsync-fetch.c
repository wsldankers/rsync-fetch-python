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

typedef struct pipestream {
	char *buf;
	size_t size; // size of the memory allocation
	size_t offset; // offset of start of data
	size_t fill; // how much space in use by data
	int fd;
} pipestream_t;
static const pipestream_t pipestream_0 = {.fd = -1};

#define RF_STREAM_IN_BUFSIZE 65536
#define RF_STREAM_OUT_BUFSIZE 65536
#define RF_STREAM_ERR_BUFSIZE 4096

enum {
	RF_STREAM_IN,
	RF_STREAM_OUT,
	RF_STREAM_ERR,
	RF_STREAM_NUM
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

typedef enum {
	RF_STATUS_OK,
	RF_STATUS_ERRNO,
	RF_STATUS_TIMEOUT,
	RF_STATUS_HANGUP,
	RF_STATUS_PREMATURE_EOF,
	RF_STATUS_ASSERT,
} rf_status_t;

#define RF_PROPAGATE_ERROR(x) do { rf_status_t __e_##__LINE__ = (x); if(__e_##__LINE__ != RF_STATUS_OK) return __e_##__LINE__; } while(0)

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
									rf->stream[RF_STREAM_IN] = pipestream_0;
									rf->stream[RF_STREAM_OUT] = pipestream_0;
									rf->stream[RF_STREAM_ERR] = pipestream_0;
									rf->stream[RF_STREAM_IN].fd = in_pipe[0];
									rf->stream[RF_STREAM_OUT].fd = out_pipe[1];
									rf->stream[RF_STREAM_ERR].fd = err_pipe[0];
									rf->stream[RF_STREAM_IN].size = RF_STREAM_IN_BUFSIZE;
									rf->stream[RF_STREAM_OUT].size = RF_STREAM_OUT_BUFSIZE;
									rf->stream[RF_STREAM_ERR].size = RF_STREAM_ERR_BUFSIZE;
									rf->multiplex = true;
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
		for(int i = 0; i < RF_STREAM_NUM; i++) {
			pipestream_t *stream = &rf->stream[i];
			if(stream->fd != -1)
				close(stream->fd);
			free(stream->buf);
		}
		if(rf->pid)
			while(waitpid(rf->pid, NULL, 0) == -1 && errno == EINTR);
	}

	freefunc tp_free = Py_TYPE(self)->tp_free ?: PyObject_Free;
	tp_free(self);

	return 0;
}

static rf_status_t rf_write_out_stream(RsyncFetch_t *rf) {
	pipestream_t *stream = &rf->stream[RF_STREAM_OUT];
	size_t fill = stream->fill;
	size_t size = stream->size;
	size_t offset = stream->offset;
	char *buf = stream->buf;

	ssize_t r;
	if(offset + fill > size) {
		size_t amount = size - offset;
		struct iovec iov[2] = {
			{ buf + offset, amount },
			{ buf, fill - amount },
		};
		r = writev(stream->fd, iov, orz(iov));
	} else {
		r = write(stream->fd, buf + offset, fill);
	}

	if(r == -1)
		return RF_STATUS_ERRNO;

	fill -= r;
	if(fill) {
		stream->fill = fill;
		offset += r;
		stream->offset = offset < size ? offset : offset - size;
	} else {
		stream->fill = 0;
		stream->offset = 0;
	}

	return RF_STATUS_OK;
}

static rf_status_t rf_read_error_stream(RsyncFetch_t *rf) {
	pipestream_t *stream = &rf->stream[RF_STREAM_ERR];
	size_t fill = stream->fill;
	size_t size = stream->size;
	char *buf = stream->buf;

	if(!buf) {
		buf = malloc(size);
		if(!buf)
			return RF_STATUS_ERRNO;
		stream->buf = buf;
	}

	char *old_buf_end = buf + fill;
	ssize_t r = read(stream->fd, old_buf_end, size - fill);
	if(r == -1)
		return RF_STATUS_ERRNO;

	char *new_buf_end = old_buf_end + r;
	char *todo = buf;
	for(char *eol = memchr(old_buf_end, '\n', r); eol++; eol = memchr(eol, '\n', new_buf_end - eol)) {
		// FIXME: handle through callback
		if(write(STDERR_FILENO, todo, eol - todo) == -1) {
			stream->fill = 0;
			return RF_STATUS_ERRNO;
		}
		todo = eol;
	}

	if(todo != buf) {
		fill = new_buf_end - todo;
		if(fill)
			memmove(buf, todo, fill);
		stream->fill = fill;
	} else if(fill + r == size) {
		stream->fill = 0;
		// FIXME: handle through callback
		if(write(STDERR_FILENO, buf, size) == -1)
			return RF_STATUS_ERRNO;
	}

	return RF_STATUS_OK;
}

static rf_status_t rf_read_bytes_raw(RsyncFetch_t *rf, char *dst, size_t len) {
	pipestream_t *stream = &rf->stream[RF_STREAM_IN];
	size_t fill = stream->fill;
	size_t size = stream->size;
	size_t offset = stream->offset;
	char *buf = stream->buf;

	if(!buf) {
		buf = malloc(size);
		if(!buf)
			return RF_STATUS_ERRNO;
		stream->buf = buf;
	}

	if(len > fill) {
		if(fill) {
			if(offset + fill > size) {
				size_t amount = size - offset;
				memcpy(dst, stream->buf + offset, amount);
				memcpy(dst + amount, buf, fill - amount);
			} else {
				memcpy(dst, buf + offset, fill);
			}
			dst += fill;
			len -= fill;
			stream->offset = 0;
			stream->fill = 0;
		}

		for(;;) {
			struct pollfd pfds[3];
			for(int i = 0; i < RF_STREAM_NUM; i++) {
				pipestream_t *stream = &rf->stream[i];
				if(i == RF_STREAM_OUT) {
					pfds[i].fd = stream->fill ? stream->fd : -1;
					pfds[i].events = POLLOUT;
				} else {
					pfds[i].fd = stream->fd;
					pfds[i].events = POLLIN;
				}
			}

			int r = poll(pfds, orz(pfds), 500);
			if(r == -1)
				return RF_STATUS_ERRNO;
			if(r == 0)
				return RF_STATUS_TIMEOUT;

			for(int i = 0; i < orz(pfds); i++) {
				if(pfds[i].revents & (POLLERR|POLLHUP)) {
					if(i == RF_STREAM_ERR) {
						pipestream_t *stream = &rf->stream[i];
						close(stream->fd);
						stream->fd = -1;
					} else {
						return RF_STATUS_HANGUP;
					}
				}
			}

			if(pfds[RF_STREAM_OUT].revents & POLLOUT) {
				RF_PROPAGATE_ERROR(rf_write_out_stream(rf));
				continue;
			}

			for(int i = 0; i < orz(pfds); i++) {
				if(pfds[i].revents & POLLOUT)
					return RF_STATUS_ASSERT;

				if(pfds[i].revents & POLLIN) {
					pipestream_t *stream = &rf->stream[i];
					if(i == RF_STREAM_IN) {
						struct iovec iov[2] = {
							{ dst, len },
							{ buf, size },
						};
						ssize_t r = readv(stream->fd, iov, orz(iov));
						if(r == -1)
							return RF_STATUS_ERRNO;
						if(r < len) {
							dst += r;
							len -= r;
						} else {
							stream->fill = r - len;
							return RF_STATUS_OK;
						}
					} else {
						if(i != RF_STREAM_ERR)
							return RF_STATUS_ASSERT;
						RF_PROPAGATE_ERROR(rf_read_error_stream(rf));
					}
				}
			}
		}
	} else {
		if(offset + len > size) {
			size_t amount = size - offset;
			memcpy(dst, buf + offset, amount);
			memcpy(dst + amount, stream->buf, len - amount);
		} else {
			memcpy(dst, buf + offset, len);
		}

		if(len == fill) {
			stream->offset = 0;
			stream->fill = 0;
		} else {
			offset += len;
			stream->offset = offset < size ? offset : offset - size;
			stream->fill = fill - len;
		}
	}

	return RF_STATUS_OK;
}

static rf_status_t wait_for_eof(RsyncFetch_t *rf) {
	for(;;) {
		if(rf->stream[RF_STREAM_IN].fill)
			return RF_STATUS_PREMATURE_EOF;

		struct pollfd pfds[3];
		for(int i = 0; i < RF_STREAM_NUM; i++) {
			pipestream_t *stream = &rf->stream[i];
			if(i == RF_STREAM_OUT) {
				pfds[i].fd = stream->fill ? stream->fd : -1;
				pfds[i].events = POLLOUT;
			} else {
				pfds[i].fd = stream->fd;
				pfds[i].events = POLLIN;
			}
		}

		int r = poll(pfds, orz(pfds), 500);
		if(r == -1)
			return RF_STATUS_ERRNO;
		if(r == 0)
			return RF_STATUS_TIMEOUT;

		for(int i = 0; i < orz(pfds); i++) {
			pipestream_t *stream = &rf->stream[i];
			if(pfds[i].revents & POLLIN) {
				if(i == RF_STREAM_IN)
					return RF_STATUS_PREMATURE_EOF;
				if(i != RF_STREAM_ERR)
					return RF_STATUS_ASSERT;
				RF_PROPAGATE_ERROR(rf_read_error_stream(rf));
			}
			if(pfds[i].revents & POLLOUT) {
				if(i != RF_STREAM_OUT)
					return RF_STATUS_ASSERT;
				RF_PROPAGATE_ERROR(rf_write_out_stream(rf));
			}
			if(pfds[i].revents & (POLLERR|POLLHUP)) {
				close(stream->fd);
				stream->fd = -1;
				if(i == RF_STREAM_IN)
					return RF_STATUS_OK;
			}
		}
	}
}

static rf_status_t rf_read_bytes(RsyncFetch_t *rf, char *buf, size_t len) {
	if(!rf->multiplex)
		return rf_read_bytes_raw(rf, buf, len);

	size_t multiplex_remaining = rf->multiplex_remaining;
	for(;;) {
		if(multiplex_remaining < len) {
			if(multiplex_remaining) {
				RF_PROPAGATE_ERROR(rf_read_bytes_raw(rf, buf, multiplex_remaining));
				buf += multiplex_remaining;
				len -= multiplex_remaining;
			}
		} else {
			rf->multiplex_remaining = multiplex_remaining - len;
			return rf_read_bytes_raw(rf, buf, len);
		}

		for(;;) {
			uint8_t mplex[4];
			RF_PROPAGATE_ERROR(rf_read_bytes_raw(rf, (char *)mplex, sizeof mplex));
			multiplex_remaining = mplex[0] | mplex[1] << 8 | mplex[2] << 16;
			uint8_t channel = mplex[3];

			if(channel == MSG_DATA)
				break;

			if(multiplex_remaining) {
				char *message = malloc(multiplex_remaining);
				if(!message)
					return false;
				rf_status_t e = rf_read_bytes_raw(rf, message, multiplex_remaining);
				if(e != RF_STATUS_OK) {
					free(message);
					return e;
				}
				// FIXME: handle message
				free(message);
			} else {
				// FIXME: handle empty message
			}
		}
	}
}

static rf_status_t rf_write_bytes_raw(RsyncFetch_t *rf, char *src, size_t len) {
	pipestream_t *stream = &rf->stream[RF_STREAM_OUT];
	size_t fill = stream->fill;
	size_t size = stream->size;
	size_t offset = stream->offset;
	char *buf = stream->buf;

	if(buf) {
		if(fill + len > size) {
			size_t newsize = size << 1;
			while(fill + len > newsize)
				newsize <<= 1;
			if(offset) {
				char *newbuf = malloc(newsize);
				if(!newbuf)
					return RF_STATUS_ERRNO;
				if(offset + fill > size) {
					size_t amount = size - offset;
					memcpy(newbuf, buf + offset, amount);
					memcpy(newbuf + amount, buf, fill - amount);
				} else {
					memcpy(newbuf, buf + offset, size);
				}
				stream->offset = offset = 0;
				free(buf);
				buf = newbuf;
			} else {
				buf = realloc(buf, newsize);
				if(!buf)
					return RF_STATUS_ERRNO;
			}
			stream->buf = buf;
			stream->size = size = newsize;
		}
	} else {
		while(len > size)
			size <<= 1;
		buf = malloc(size);
		if(!buf)
			return RF_STATUS_ERRNO;
		stream->buf = buf;
	}

	size_t start = offset + fill;
	if(start > size)
		start -= size;

	if(start + len > size) {
		size_t amount = size - start;
		memcpy(buf + start, src, amount);
		memcpy(buf, src + amount, len - amount);
	} else {
		memcpy(buf + start, src, len);
	}

	stream->fill = fill + len;

	return RF_STATUS_OK;
}

static rf_status_t rf_write_bytes(RsyncFetch_t *rf, char *buf, size_t len) {
	if(!rf->multiplex)
		return rf_write_bytes_raw(rf, buf, len);
	while(len) {
		size_t chunk = len < 0xFFFFFF ? len : 0xFFFFFF;
		uint8_t mplex[4] = { chunk, chunk >> 8, chunk >> 16, MSG_DATA };
		RF_PROPAGATE_ERROR(rf_write_bytes_raw(rf, (char *)mplex, sizeof mplex));
		RF_PROPAGATE_ERROR(rf_write_bytes_raw(rf, buf, chunk));
		len -= chunk;
		buf += chunk;
	}
	return RF_STATUS_OK;
}

static rf_status_t send_int8(RsyncFetch_t *rf, int8_t d) {
	return rf_write_bytes(rf, (char *)&d, sizeof d);
}

static rf_status_t send_uint8(RsyncFetch_t *rf, uint8_t d) {
	return rf_write_bytes(rf, (char *)&d, sizeof d);
}

static rf_status_t send_int16(RsyncFetch_t *rf, int16_t d) {
	int16_t le = le16(d);
	return rf_write_bytes(rf, (char *)&le, sizeof le);
}

static rf_status_t send_uint16(RsyncFetch_t *rf, uint16_t d) {
	uint16_t le = le16(d);
	return rf_write_bytes(rf, (char *)&le, sizeof le);
}

static rf_status_t send_int32(RsyncFetch_t *rf, int32_t d) {
	int32_t le = le32(d);
	return rf_write_bytes(rf, (char *)&le, sizeof le);
}

static rf_status_t send_uint32(RsyncFetch_t *rf, uint32_t d) {
	uint32_t le = le32(d);
	return rf_write_bytes(rf, (char *)&le, sizeof le);
}

static rf_status_t send_int64(RsyncFetch_t *rf, int64_t d) {
	int64_t le = le64(d);
	return rf_write_bytes(rf, (char *)&le, sizeof le);
}

static rf_status_t send_uint64(RsyncFetch_t *rf, uint64_t d) {
	uint64_t le = le64(d);
	return rf_write_bytes(rf, (char *)&le, sizeof le);
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

	if(rf_read_bytes(rf, PyBytes_AsString(ret), howmany) == RF_STATUS_OK)
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

	if(rf_write_bytes(rf, buf, len) == RF_STATUS_OK)
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

