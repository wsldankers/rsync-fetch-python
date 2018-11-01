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
#define PIPESTREAM_INITIALIZER {.fd = -1}
static const pipestream_t pipestream_0 = PIPESTREAM_INITIALIZER;

#define RF_STREAM_IN_BUFSIZE 65536
#define RF_STREAM_OUT_BUFSIZE 65536
#define RF_STREAM_ERR_BUFSIZE 4096
#define RF_BUFSIZE_ADJUSTMENT (3 * sizeof(void *))

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
	RF_STATUS_PYTHON,
	RF_STATUS_TIMEOUT,
	RF_STATUS_HANGUP,
	RF_STATUS_ASSERT,
	RF_STATUS_PROTO,
} rf_status_t;

#define RF_PROPAGATE_ERROR(x) do { rf_status_t __e_##__LINE__ = (x); if(__e_##__LINE__ != RF_STATUS_OK) return __e_##__LINE__; } while(0)

#define RSYNCFETCH_MAGIC UINT64_C(0x6FB32179D3F495D0)

typedef struct RsyncFetch {
	uint64_t magic;
	pipestream_t stream[3];
	size_t multiplex_remaining;
	int32_t prev_negative_ndx_in;
	int32_t prev_positive_ndx_in;
	int32_t prev_negative_ndx_out;
	int32_t prev_positive_ndx_out;
	int pid;
	bool multiplex;
	bool failed;
	bool closed;
} RsyncFetch_t;

static const RsyncFetch_t RsyncFetch_0 = {
	.magic = RSYNCFETCH_MAGIC,
	.stream = { PIPESTREAM_INITIALIZER, PIPESTREAM_INITIALIZER, PIPESTREAM_INITIALIZER },
	.multiplex = false,
};

struct RsyncFetchObject {
	PyObject_HEAD
	RsyncFetch_t rf;
};

static struct PyModuleDef rsync_fetch_module;
static PyTypeObject RsyncFetch_type;

static inline RsyncFetch_t *RsyncFetch_Check(PyObject *v, bool check_failed) {
	if(v && PyObject_TypeCheck(v, &RsyncFetch_type)
	&& ((struct RsyncFetchObject *)v)->rf.magic == RSYNCFETCH_MAGIC) {
		RsyncFetch_t *rf = &((struct RsyncFetchObject *)v)->rf;
		if(check_failed && rf->failed) {
			PyErr_Format(PyExc_RuntimeError, "RsyncFetch object is in failed state");
			return NULL;
		} else {
			return rf;
		}
	} else {
		PyErr_Format(PyExc_TypeError, "not a valid RsyncFetch object");
		return NULL;
	}
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

								struct RsyncFetchObject *obj = PyObject_New(struct RsyncFetchObject, subtype);
								if(obj) {
									RsyncFetch_t *rf = &obj->rf;
									*rf = RsyncFetch_0;
									rf->pid = pid;
									rf->stream[RF_STREAM_IN].fd = in_pipe[0];
									rf->stream[RF_STREAM_OUT].fd = out_pipe[1];
									rf->stream[RF_STREAM_ERR].fd = err_pipe[0];
									rf->stream[RF_STREAM_IN].size = RF_STREAM_IN_BUFSIZE - RF_BUFSIZE_ADJUSTMENT;
									rf->stream[RF_STREAM_OUT].size = RF_STREAM_OUT_BUFSIZE - RF_BUFSIZE_ADJUSTMENT;
									rf->stream[RF_STREAM_ERR].size = RF_STREAM_ERR_BUFSIZE - RF_BUFSIZE_ADJUSTMENT;

									return &obj->ob_base;
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
	RsyncFetch_t *rf = RsyncFetch_Check(self, false);
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

static rf_status_t rf_recv_bytes_raw(RsyncFetch_t *rf, char *dst, size_t len) {
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

__attribute__((unused))
static rf_status_t wait_for_eof(RsyncFetch_t *rf) {
	for(;;) {
		if(rf->stream[RF_STREAM_IN].fill)
			return RF_STATUS_PROTO;

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
					return RF_STATUS_PROTO;
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

static rf_status_t rf_recv_bytes(RsyncFetch_t *rf, char *buf, size_t len) {
	if(!rf->multiplex)
		return rf_recv_bytes_raw(rf, buf, len);

	size_t multiplex_remaining = rf->multiplex_remaining;
	for(;;) {
		if(multiplex_remaining < len) {
			if(multiplex_remaining) {
				RF_PROPAGATE_ERROR(rf_recv_bytes_raw(rf, buf, multiplex_remaining));
				buf += multiplex_remaining;
				len -= multiplex_remaining;
			}
		} else {
			rf->multiplex_remaining = multiplex_remaining - len;
			return rf_recv_bytes_raw(rf, buf, len);
		}

		for(;;) {
			uint8_t mplex[4];
			RF_PROPAGATE_ERROR(rf_recv_bytes_raw(rf, (char *)mplex, sizeof mplex));
			multiplex_remaining = mplex[0] | mplex[1] << 8 | mplex[2] << 16;
			uint8_t channel = mplex[3];

			if(channel == MSG_DATA)
				break;

			if(multiplex_remaining) {
				char *message = malloc(multiplex_remaining);
				if(!message)
					return false;
				rf_status_t e = rf_recv_bytes_raw(rf, message, multiplex_remaining);
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

static rf_status_t rf_send_bytes_raw(RsyncFetch_t *rf, char *src, size_t len) {
	pipestream_t *stream = &rf->stream[RF_STREAM_OUT];
	size_t fill = stream->fill;
	size_t size = stream->size;
	size_t offset = stream->offset;
	char *buf = stream->buf;

	if(buf) {
		if(fill + len > size) {
			size_t newsize = (size + RF_BUFSIZE_ADJUSTMENT) << 1;
			while(fill + len > newsize)
				newsize <<= 1;
			newsize -= RF_BUFSIZE_ADJUSTMENT;
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
		size += RF_BUFSIZE_ADJUSTMENT;
		while(len > size)
			size <<= 1;
		size -= RF_BUFSIZE_ADJUSTMENT;
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

static rf_status_t rf_send_bytes(RsyncFetch_t *rf, char *buf, size_t len) {
	if(!rf->multiplex)
		return rf_send_bytes_raw(rf, buf, len);
	while(len) {
		size_t chunk = len < 0xFFFFFF ? len : 0xFFFFFF;
		uint8_t mplex[4] = { chunk, chunk >> 8, chunk >> 16, MSG_DATA };
		RF_PROPAGATE_ERROR(rf_send_bytes_raw(rf, (char *)mplex, sizeof mplex));
		RF_PROPAGATE_ERROR(rf_send_bytes_raw(rf, buf, chunk));
		len -= chunk;
		buf += chunk;
	}
	return RF_STATUS_OK;
}

__attribute__((unused))
static rf_status_t rf_send_int8(RsyncFetch_t *rf, int8_t d) {
	return rf_send_bytes(rf, (char *)&d, sizeof d);
}

__attribute__((unused))
static rf_status_t rf_send_uint8(RsyncFetch_t *rf, uint8_t d) {
	return rf_send_bytes(rf, (char *)&d, sizeof d);
}

__attribute__((unused))
static rf_status_t rf_send_int16(RsyncFetch_t *rf, int16_t d) {
	int16_t le = le16(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static rf_status_t rf_send_uint16(RsyncFetch_t *rf, uint16_t d) {
	uint16_t le = le16(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static rf_status_t rf_send_int32(RsyncFetch_t *rf, int32_t d) {
	int32_t le = le32(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static rf_status_t rf_send_uint32(RsyncFetch_t *rf, uint32_t d) {
	uint32_t le = le32(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static rf_status_t rf_send_int64(RsyncFetch_t *rf, int64_t d) {
	int64_t le = le64(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static rf_status_t rf_send_uint64(RsyncFetch_t *rf, uint64_t d) {
	uint64_t le = le64(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static rf_status_t rf_recv_int8(RsyncFetch_t *rf, int8_t *d) {
	return rf_recv_bytes(rf, (char *)d, sizeof *d);
}

__attribute__((unused))
static rf_status_t rf_recv_uint8(RsyncFetch_t *rf, uint8_t *d) {
	return rf_recv_bytes(rf, (char *)d, sizeof *d);
}

__attribute__((unused))
static rf_status_t rf_recv_int16(RsyncFetch_t *rf, int16_t *d) {
	int16_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le16(le);
	return RF_STATUS_OK;
}

__attribute__((unused))
static rf_status_t rf_recv_uint16(RsyncFetch_t *rf, uint16_t *d) {
	uint16_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le16(le);
	return RF_STATUS_OK;
}

__attribute__((unused))
static rf_status_t rf_recv_int32(RsyncFetch_t *rf, int32_t *d) {
	int32_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le32(le);
	return RF_STATUS_OK;
}

__attribute__((unused))
static rf_status_t rf_recv_uint32(RsyncFetch_t *rf, uint32_t *d) {
	uint32_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le32(le);
	return RF_STATUS_OK;
}

__attribute__((unused))
static rf_status_t rf_recv_int64(RsyncFetch_t *rf, int64_t *d) {
	int64_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le64(le);
	return RF_STATUS_OK;
}

__attribute__((unused))
static rf_status_t rf_recv_uint64(RsyncFetch_t *rf, uint64_t *d) {
	uint64_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le64(le);
	return RF_STATUS_OK;
}

static const uint8_t varint_extra[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // (00 - 3F)/4
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // (40 - 7F)/4
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // (80 - BF)/4
	2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 5, 6, // (C0 - FF)/4
};

__attribute__((unused))
static rf_status_t recv_varint(RsyncFetch_t *rf, uint32_t *d) {
	uint8_t init;
	RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &init));
	size_t extra = varint_extra[init >> 2];
	if(extra) {
		uint8_t extra_bytes[7];
		RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)extra_bytes, extra));
		extra_bytes[extra] = (uint32_t)init & ((1 << (8 - extra)) - 1);
		uint32_t v = 0;
		for(int i = 0; i <= extra; i++)
			v |= extra_bytes[i] << (8 * i);
		*d = v;
	} else {
		*d = init;
	}
	return RF_STATUS_OK;
}

__attribute__((unused))
static rf_status_t recv_varlong(RsyncFetch_t *rf, uint32_t *d, size_t min_bytes) {
	if(min_bytes > 8)
		return RF_STATUS_ASSERT;
	uint8_t init_bytes[9] = {0};
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)init_bytes, min_bytes));
	size_t extra = varint_extra[init_bytes[0] >> 2];
	uint8_t *extra_bytes = init_bytes + 1;
	size_t total_bytes = min_bytes + extra;
	if(extra) {
		if(total_bytes >= sizeof(uint64_t))
			return RF_STATUS_PROTO;
		RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)extra_bytes + min_bytes, extra));
	}
	extra_bytes[total_bytes] = (uint32_t)init_bytes[0] & ((1 << (8 - extra)) - 1);
	uint64_t v = 0;
	for(int i = 0; i <= total_bytes; i++)
		v |= extra_bytes[i] << (8 * i);
	*d = v;
	return RF_STATUS_OK;
}

static bool rf_status_to_exception(RsyncFetch_t *rf, rf_status_t s) {
	switch(s) {
		case RF_STATUS_OK:
			return true;
		case RF_STATUS_ERRNO:
			PyErr_SetFromErrno(PyExc_OSError);
			break;
		case RF_STATUS_PYTHON:
			break;
		case RF_STATUS_TIMEOUT:
			return PyErr_Format(PyExc_RuntimeError, "operation timed out");
			break;
		case RF_STATUS_HANGUP:
			return PyErr_Format(PyExc_RuntimeError, "process exited prematurely");
			break;
		case RF_STATUS_PROTO:
			return PyErr_Format(PyExc_RuntimeError, "protocol error");
			break;
		default:
		case RF_STATUS_ASSERT:
			return PyErr_Format(PyExc_RuntimeError, "internal error");
			break;
	}
	rf->failed = true;
	return false;
}

static PyObject *RsyncFetch_readbytes(PyObject *self, PyObject *howmany_obj) {
	PyErr_Clear();
	Py_ssize_t howmany = PyNumber_AsSsize_t(howmany_obj, PyExc_OverflowError);
	if(howmany == -1 && PyErr_Occurred())
		return NULL;
	if(howmany < 0)
		return PyErr_Format(PyExc_ValueError, "cannot read a negative number of bytes");

	RsyncFetch_t *rf = RsyncFetch_Check(self, true);
	if(!rf)
		return NULL;

	PyObject *ret = PyBytes_FromStringAndSize(NULL, howmany);
	if(!ret)
		return NULL;

	if(rf_status_to_exception(rf, rf_recv_bytes(rf, PyBytes_AsString(ret), howmany)))
		return ret;

	Py_DecRef(ret);
	return NULL;
}

static PyObject *RsyncFetch_writebytes(PyObject *self, PyObject *bytes) {
	RsyncFetch_t *rf = RsyncFetch_Check(self, true);
	if(!rf)
		return NULL;

	char *buf;
	Py_ssize_t len;
	if(PyBytes_AsStringAndSize(bytes, &buf, &len) == -1)
		return NULL;

//fprintf(stderr, "%s:%d: len=%zd\n", __FILE__, __LINE__, len);

	if(rf_status_to_exception(rf, rf_send_bytes(rf, buf, len)))
		Py_RETURN_NONE;

	return NULL;
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
	.tp_basicsize = sizeof(struct RsyncFetchObject),
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

