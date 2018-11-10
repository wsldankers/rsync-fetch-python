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

typedef struct rf_pipestream {
	char *buf;
	size_t size; // size of the memory allocation
	size_t offset; // offset of start of data
	size_t fill; // how much space in use by data
	int fd;
} rf_pipestream_t;
#define PIPESTREAM_INITIALIZER {.fd = -1}
static const rf_pipestream_t rf_pipestream_0 = PIPESTREAM_INITIALIZER;

#define RF_STREAM_IN_BUFSIZE 65536
#define RF_STREAM_OUT_BUFSIZE 65536
#define RF_STREAM_ERR_BUFSIZE 4096
#define RF_BUFNUM_ADJUSTMENT (3)
#define RF_BUFSIZE_ADJUSTMENT (RF_BUFNUM_ADJUSTMENT * sizeof(void *))

#define MAX_BLOCK_SIZE 131072
#define MPLEX_BASE 7

#define NDX_DONE INT32_C(1)
#define NDX_FLIST_EOF INT32_C(-2)
#define NDX_FLIST_OFFSET INT32_C(-101)

#define XMIT_TOP_DIR (1 << 0)
#define XMIT_SAME_MODE (1 << 1)
#define XMIT_EXTENDED_FLAGS (1 << 2)
#define XMIT_SAME_UID (1 << 3)
#define XMIT_SAME_GID (1 << 4)
#define XMIT_SAME_NAME (1 << 5)
#define XMIT_LONG_NAME (1 << 6)
#define XMIT_SAME_TIME (1 << 7)
#define XMIT_SAME_RDEV_MAJOR (1 << 8)
#define XMIT_NO_CONTENT_DIR (1 << 8)
#define XMIT_HLINKED (1 << 9)
#define XMIT_USER_NAME_FOLLOWS (1 << 10)
#define XMIT_GROUP_NAME_FOLLOWS (1 << 11)
#define XMIT_HLINK_FIRST (1 << 12)
#define XMIT_IO_ERROR_ENDLIST (1 << 12)

#define ITEM_REPORT_CHANGE (1 << 1)
#define ITEM_REPORT_SIZE (1 << 2)
#define ITEM_REPORT_TIMEFAIL (1 << 2)
#define ITEM_REPORT_TIME (1 << 3)
#define ITEM_REPORT_PERMS (1 << 4)
#define ITEM_REPORT_OWNER (1 << 5)
#define ITEM_REPORT_GROUP (1 << 6)
#define ITEM_REPORT_ACL (1 << 7)
#define ITEM_REPORT_XATTR (1 << 8)
#define ITEM_BASIS_TYPE_FOLLOWS (1 << 11)
#define ITEM_XNAME_FOLLOWS (1 << 12)
#define ITEM_IS_NEW (1 << 13)
#define ITEM_LOCAL_CHANGE (1 << 14)
#define ITEM_TRANSFER (1 << 15)

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
	MSG_LOG = 6,
	MSG_NOOP = 42,
	MSG_DELETED = 101,

	// integer type:
	MSG_IO_ERROR = 22,
	MSG_SUCCESS = 100,
	MSG_NO_SEND = 102,

	// rsync internal only (we should never see these):
	MSG_ERROR_SOCKET = 5,
	MSG_CLIENT = 7,
	MSG_ERROR_UTF8 = 8,
	MSG_REDO = 9,
	MSG_FLIST = 20,
	MSG_FLIST_EOF = 21,
	MSG_DONE = 86,
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

typedef struct rf_flist_entry {
	char *name;
	char *user;
	char *group;
	char *symlink;
	char *hardlink;
	PyObject *data_callback;
	int64_t size;
	int64_t mtime;
	int32_t mode;
	int32_t uid;
	int32_t gid;
	int32_t major;
	int32_t minor;
	int32_t ndx;
} rf_flist_entry_t;
static const rf_flist_entry_t rf_flist_entry_0;

typedef struct rf_flist {
	struct rf_flist *prev;
	struct rf_flist *next;
	size_t size;
	size_t num;
	size_t offset;
	rf_flist_entry_t **entries;
} rf_flist_t;
static const rf_flist_t rf_flist_0;

#define RF_PROPAGATE_ERROR(x) do { rf_status_t __e_##__LINE__ = (x); if(__e_##__LINE__ != RF_STATUS_OK) return __e_##__LINE__; } while(false)

#define RSYNCFETCH_MAGIC UINT64_C(0x6FB32179D3F495D0)

typedef struct RsyncFetch {
	uint64_t magic;
#ifdef WITH_THREAD
	PyThreadState *py_thread_state;
	PyThread_type_lock lock;
#endif
	PyObject *entry_callback;
	PyObject *error_callback;
	PyObject *chunk_bytes;
	char *chunk_buffer;
	char **command;
	char **filters;
	size_t filters_num;
	rf_flist_t *flist;
	rf_flist_t *flists_head;
	rf_flist_t *flists_tail;
	rf_pipestream_t in_stream;
	rf_pipestream_t out_stream;
	rf_pipestream_t err_stream;
	size_t flists_num;
	size_t flists_size;
	rf_flist_entry_t last;
	size_t multiplex_in_remaining;
	size_t multiplex_out_remaining;
	size_t chunk_size;
	int32_t ndx;
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
	.in_stream = PIPESTREAM_INITIALIZER,
	.out_stream = PIPESTREAM_INITIALIZER,
	.err_stream = PIPESTREAM_INITIALIZER,
	.ndx = 1,
	.prev_negative_ndx_in = 1,
	.prev_positive_ndx_in = -1,
	.prev_negative_ndx_out = 1,
	.prev_positive_ndx_out = -1,
	.chunk_size = 32768,
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

#ifdef WITH_THREAD

static inline bool rf_unblock_threads(RsyncFetch_t *rf) {
	if(rf->py_thread_state)
		return false;
	rf->py_thread_state = PyEval_SaveThread();
	return true;
}

static inline bool rf_block_threads(RsyncFetch_t *rf) {
	PyThreadState *py_thread_state = rf->py_thread_state;
	if(py_thread_state) {
		rf->py_thread_state = NULL;
		PyEval_RestoreThread(py_thread_state);
		return true;
	}
	return false;
}

#else

static void rf_block_threads(RsyncFetch_t *rf) {}
static void rf_unblock_threads(RsyncFetch_t *rf) {}

#endif

struct refstring_header {
	size_t len;
	size_t refcount;
};

static inline size_t rf_refstring_len(const char *str) {
	return str ? ((struct refstring_header *)str)[-1].len : 0;
}

static rf_status_t rf_refstring_newlen(RsyncFetch_t *rf, const char *str, size_t len, char **strp) {
	struct refstring_header *h = malloc(sizeof *h + len + 1);
	if(!h)
		return RF_STATUS_ERRNO;
	if(str)
		memcpy(h + 1, str, len);
	((char *)(h + 1))[len] = '\0';
	h->len = len;
	h->refcount = 1;
	return *strp = (char *)(h + 1), RF_STATUS_OK;
}

__attribute__((unused))
static rf_status_t rf_refstring_new(RsyncFetch_t *rf, const char *str, char **strp) {
	if(str)
		return rf_refstring_newlen(rf, str, strlen(str), strp);
	else
		return RF_STATUS_OK;
}

static rf_status_t rf_refstring_free(RsyncFetch_t *rf, char **strp) {
	if(strp) {
		char *str = *strp;
		if(str) {
			*strp = NULL;
			struct refstring_header *h = (struct refstring_header *)str - 1;
			size_t refcount = h->refcount;
			if(refcount == 1)
				free(h);
			else
				h->refcount = refcount - 1;
		}
	}
	return RF_STATUS_OK;
}

static rf_status_t rf_refstring_dup(RsyncFetch_t *rf, char *str, char **strp) {
	RF_PROPAGATE_ERROR(rf_refstring_free(rf, strp));
	if(str) {
		struct refstring_header *h = (struct refstring_header *)str - 1;
		h->refcount++;
		if(strp)
			*strp = str;
	}
	return RF_STATUS_OK;
}

static rf_status_t rf_ensure_bytes(RsyncFetch_t *rf, PyObject *obj, PyObject **bytesp) {
	if(PyUnicode_Check(obj)) {
		PyObject *bytes = PyUnicode_AsEncodedString(obj, "UTF-8", "surrogateescape");
		if(!bytes)
			return RF_STATUS_PYTHON;
		*bytesp = bytes;
	} else if(PyBytes_Check(obj)) {
		Py_IncRef(obj);
		*bytesp = obj;
	} else {
		PyObject *bytes = PyBytes_FromObject(obj);
		if(!bytes)
			return RF_STATUS_PYTHON;
		*bytesp = bytes;
	}
	return RF_STATUS_OK;
}

static rf_status_t rf_iterate(RsyncFetch_t *rf, PyObject *iterable, char ***listp, size_t *countp) {
	PyObject *iterator = PyObject_GetIter(iterable);
	if(!iterator)
		return RF_STATUS_PYTHON;

	rf_status_t s = RF_STATUS_OK;

	size_t fill = 0;
	size_t size = 32;
	while(size < RF_BUFNUM_ADJUSTMENT)
		size <<= 1;
	size -= RF_BUFNUM_ADJUSTMENT;
	void **list = malloc(size * sizeof *list);

	if(list) {
		for(;;) {
			PyObject *item = PyIter_Next(iterator);
			if(!item) {
				if(PyErr_Occurred())
					s = RF_STATUS_PYTHON;
				break;
			}
			PyObject *bytes;

			s = rf_ensure_bytes(rf, item, &bytes);
			Py_DecRef(item);
			if(s != RF_STATUS_OK)
				break;

			if(fill == size) {
				size = ((size + RF_BUFNUM_ADJUSTMENT) << 1) - RF_BUFNUM_ADJUSTMENT;
				void **newlist = realloc(list, size * sizeof *list);
				if(!newlist) {
					s = RF_STATUS_ERRNO;
					break;
				}
				list = newlist;
			}

			list[fill++] = bytes;
		}

		// OK, we now have a list with bytes items (we hope).
		// The idea is now to convert it to a single memory
		// allocation that starts with the actual list of string
		// pointers, followed by the strings themselves.

		size_t total = 0;
		if(s == RF_STATUS_OK) {
			for(size_t i = 0; i < fill; i++) {
				Py_ssize_t len = PyBytes_Size(list[i]);
				if(i == -1) {
					s = RF_STATUS_PYTHON;
					break;
				}
				total += len;
			}
		}

		size_t converted = 0;
		if(s == RF_STATUS_OK) {
			// tack on an extra '+ fill' to accomodate the \0 for each string
			void **newlist = realloc(list, (fill + 1) * sizeof *list + total + fill);
			if(newlist) {
				list = newlist;
				char *string_location = (char *)(list + fill + 1);
				for(size_t i = 0; i < fill; i++) {
					Py_ssize_t len;
					char *buf;
					if(PyBytes_AsStringAndSize(list[i], &buf, &len) == -1) {
						s = RF_STATUS_PYTHON;
						break;
					}
					Py_DecRef(list[i]);
					list[i] = string_location;
					converted++;
					len++; // include the trailing \0
					memcpy(string_location, buf, len);
					string_location += len;
				}
				list[fill] = string_location;
			} else {
				s = RF_STATUS_ERRNO;
			}
		}

		if(s == RF_STATUS_OK) {
			*listp = (char **)list;
			if(countp)
				*countp = fill;
			else
				list[fill] = NULL;
		} else {
			for(size_t i = converted; i < fill; i++)
				Py_DecRef(list[i]);
			free(list);
		}
	} else {
		s = RF_STATUS_ERRNO;
	}
	Py_DecRef(iterator);

	return s;
}

static rf_flist_entry_t *rf_flist_get_entry(RsyncFetch_t *rf, rf_flist_t *flist, int32_t ndx) {
	size_t offset = flist->offset;
	if(ndx >= offset) {
		ndx -= offset;
		size_t size = flist->size;
		if(ndx < size)
			return flist->entries[ndx];
	}
	return NULL;
}

static rf_flist_entry_t *rf_find_ndx(RsyncFetch_t *rf, int32_t ndx) {
	for(rf_flist_t *flist = rf->flists_tail; flist; flist = flist->prev)
		if(ndx >= flist->offset)
			return rf_flist_get_entry(rf, flist, ndx);

	return NULL;
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

static rf_status_t rf_flush_output(RsyncFetch_t *rf) {
	size_t multiplex_out_remaining = rf->multiplex_out_remaining;
	if(multiplex_out_remaining) {
		rf_pipestream_t *stream = &rf->out_stream;
		size_t start = stream->offset + stream->fill - multiplex_out_remaining - 4;
		size_t size = stream->size;
		char *buf = stream->buf;
		if(start >= size) {
			start -= size;
			buf[start] = multiplex_out_remaining;
			buf[start + 1] = multiplex_out_remaining >> 8;
			buf[start + 2] = multiplex_out_remaining >> 16;
		} else {
			switch(size - start) {
				case 1:
					buf[start] = multiplex_out_remaining;
					buf[0] = multiplex_out_remaining >> 8;
					buf[1] = multiplex_out_remaining >> 16;
				break;
				case 2:
					buf[start] = multiplex_out_remaining;
					buf[start + 1] = multiplex_out_remaining >> 8;
					buf[0] = multiplex_out_remaining >> 16;
				break;
				default:
					buf[start] = multiplex_out_remaining;
					buf[start + 1] = multiplex_out_remaining >> 8;
					buf[start + 2] = multiplex_out_remaining >> 16;
			}
		}
		
		rf->multiplex_out_remaining = 0;
	}
	return RF_STATUS_OK;
}

static rf_status_t rf_write_out_stream(RsyncFetch_t *rf) {
	rf_pipestream_t *stream = &rf->out_stream;
	size_t fill = stream->fill;
	size_t size = stream->size;
	size_t offset = stream->offset;
	char *buf = stream->buf;

	size_t multiplex_out_remaining = rf->multiplex_out_remaining;
	if(multiplex_out_remaining)
		RF_PROPAGATE_ERROR(rf_flush_output(rf));

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
		if(multiplex_out_remaining && fill >= multiplex_out_remaining + 4)
			rf->multiplex_out_remaining = multiplex_out_remaining;
	} else {
		stream->fill = 0;
		stream->offset = 0;
	}

	return RF_STATUS_OK;
}

static rf_status_t rf_read_error_stream(RsyncFetch_t *rf) {
	rf_pipestream_t *stream = &rf->err_stream;
	size_t fill = stream->fill;
	size_t size = stream->size;
	char *buf = stream->buf;

	if(!buf) {
		size = RF_STREAM_ERR_BUFSIZE - RF_BUFSIZE_ADJUSTMENT;
		buf = malloc(size);
		if(!buf)
			return RF_STATUS_ERRNO;
		stream->buf = buf;
		stream->size = size;
	}

	char *old_buf_end = buf + fill;
	ssize_t r = read(stream->fd, old_buf_end, size - fill);
	if(r == -1)
		return RF_STATUS_ERRNO;

	char *new_buf_end = old_buf_end + r;
	char *todo = buf;
	for(char *eol = memchr(old_buf_end, '\n', r); eol++; eol = memchr(eol, '\n', new_buf_end - eol)) {
		PyObject *error_callback = rf->error_callback;
		if(error_callback) {
			rf_block_threads(rf);
			PyObject *args = Py_BuildValue("(y#)",
				todo, (Py_ssize_t)(eol - todo));
			if(!args)
				return RF_STATUS_PYTHON;
			PyObject *result = PyObject_CallObject(error_callback, args);
			Py_DecRef(args);
			if(!result)
				return RF_STATUS_PYTHON;
			Py_DecRef(result);
		} else {
			if(write(STDERR_FILENO, todo, eol - todo) == -1)
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
		PyObject *error_callback = rf->error_callback;
		if(error_callback) {
			rf_block_threads(rf);
			PyObject *args = Py_BuildValue("(y#)", buf, size);
			if(!args)
				return RF_STATUS_PYTHON;
			PyObject *result = PyObject_CallObject(error_callback, args);
			Py_DecRef(args);
			if(!result)
				return RF_STATUS_PYTHON;
			Py_DecRef(result);
		} else {
			if(write(STDERR_FILENO, buf, size) == -1)
				return RF_STATUS_ERRNO;
		}
	}

	rf_unblock_threads(rf);

	return RF_STATUS_OK;
}

__attribute__((hot))
static rf_status_t rf_recv_bytes_raw(RsyncFetch_t *rf, char *dst, size_t len) {
	rf_pipestream_t *stream = &rf->in_stream;
	size_t fill = stream->fill;
	size_t size = stream->size;
	size_t offset = stream->offset;
	char *buf = stream->buf;

	if(!buf) {
		size = RF_STREAM_IN_BUFSIZE - RF_BUFSIZE_ADJUSTMENT;
		buf = malloc(size);
		if(!buf)
			return RF_STATUS_ERRNO;
		stream->buf = buf;
		stream->size = size;
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

		rf_pipestream_t *out_stream = &rf->out_stream;
		rf_pipestream_t *err_stream = &rf->err_stream;
		int fd = stream->fd;
		int out_fd = out_stream->fd;
		int err_fd = err_stream->fd;

		for(;;) {
			struct pollfd pfds[3] = {
				{ .fd = fd, .events = POLLIN },
				{ .fd = out_stream->fill ? out_fd : -1, .events = POLLOUT },
				{ .fd = err_fd, .events = POLLIN },
			};

			int r = poll(pfds, orz(pfds), 500);
			if(r == -1)
				return RF_STATUS_ERRNO;
			if(r == 0)
				return RF_STATUS_TIMEOUT;

			if(pfds[RF_STREAM_IN].revents & (POLLERR|POLLHUP))
				return RF_STATUS_HANGUP;

			if(pfds[RF_STREAM_OUT].revents & (POLLERR|POLLHUP))
				return RF_STATUS_HANGUP;

			if(pfds[RF_STREAM_ERR].revents & (POLLERR|POLLHUP)) {
				close(err_fd);
				err_stream->fd = err_fd = -1;
			}

			if(pfds[RF_STREAM_ERR].revents & POLLOUT) {
				RF_PROPAGATE_ERROR(rf_read_error_stream(rf));
				continue;
			}

			if(pfds[RF_STREAM_OUT].revents & POLLOUT) {
				RF_PROPAGATE_ERROR(rf_write_out_stream(rf));
				continue;
			}

			if(pfds[RF_STREAM_IN].revents & POLLIN) {
				struct iovec iov[2] = {
					{ dst, len },
					{ buf, size },
				};
				ssize_t r = readv(fd, iov, orz(iov));
				if(r == -1)
					return RF_STATUS_ERRNO;
				if(r < len) {
					dst += r;
					len -= r;
				} else {
					stream->fill = r - len;
					return RF_STATUS_OK;
				}
			}
		}
	} else {
		if(len == 1) {
			*dst = buf[offset];
		} else if(offset + len > size) {
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

static rf_status_t rf_wait_for_eof(RsyncFetch_t *rf) {
	rf_pipestream_t *stream = &rf->in_stream;
	rf_pipestream_t *out_stream = &rf->out_stream;
	rf_pipestream_t *err_stream = &rf->err_stream;
	int fd = stream->fd;
	int out_fd = out_stream->fd;
	int err_fd = err_stream->fd;

	for(;;) {
		if(stream->fill)
			return RF_STATUS_PROTO;

		struct pollfd pfds[3] = {
			{ .fd = fd, .events = POLLIN },
			{ .fd = out_stream->fill ? out_fd : -1, .events = POLLOUT },
			{ .fd = err_fd, .events = POLLIN },
		};

		int r = poll(pfds, orz(pfds), 500);
		if(r == -1)
			return RF_STATUS_ERRNO;
		if(r == 0)
			return RF_STATUS_TIMEOUT;

		if(pfds[RF_STREAM_ERR].revents & POLLIN)
			RF_PROPAGATE_ERROR(rf_read_error_stream(rf));

		if(pfds[RF_STREAM_OUT].revents & POLLOUT)
			RF_PROPAGATE_ERROR(rf_write_out_stream(rf));

		if(pfds[RF_STREAM_IN].revents & POLLIN)
			return RF_STATUS_PROTO;

		if(pfds[RF_STREAM_ERR].revents & (POLLERR|POLLHUP)) {
			close(err_fd);
			err_stream->fd = -1;
		}

		if(pfds[RF_STREAM_OUT].revents & (POLLERR|POLLHUP)) {
			close(out_fd);
			out_stream->fd = -1;
		}

		if(pfds[RF_STREAM_IN].revents & (POLLERR|POLLHUP)) {
			close(fd);
			stream->fd = -1;
			return RF_STATUS_OK;
		}
	}
}

__attribute__((hot))
static rf_status_t rf_recv_bytes(RsyncFetch_t *rf, char *buf, size_t len) {
	if(!rf->multiplex)
		return rf_recv_bytes_raw(rf, buf, len);

	size_t multiplex_in_remaining = rf->multiplex_in_remaining;
	for(;;) {
		if(multiplex_in_remaining < len) {
			if(multiplex_in_remaining) {
				RF_PROPAGATE_ERROR(rf_recv_bytes_raw(rf, buf, multiplex_in_remaining));
				buf += multiplex_in_remaining;
				len -= multiplex_in_remaining;
			}
		} else {
			rf->multiplex_in_remaining = multiplex_in_remaining - len;
			return rf_recv_bytes_raw(rf, buf, len);
		}

		for(;;) {
			uint8_t mplex[4];
			RF_PROPAGATE_ERROR(rf_recv_bytes_raw(rf, (char *)mplex, sizeof mplex));
			multiplex_in_remaining = mplex[0] | mplex[1] << 8 | mplex[2] << 16;
			int channel = (int)mplex[3] - MPLEX_BASE;

			if(channel == MSG_DATA)
				break;

			switch(channel) {
				case MSG_ERROR_XFER:
				case MSG_INFO:
				case MSG_ERROR:
				case MSG_WARNING:
				case MSG_LOG:
				case MSG_DELETED:
					if(multiplex_in_remaining) {
						char *message = malloc(multiplex_in_remaining);
						if(!message)
							return RF_STATUS_ERRNO;
						rf_status_t e = rf_recv_bytes_raw(rf, message, multiplex_in_remaining);
						if(e != RF_STATUS_OK) {
							free(message);
							return e;
						}

						PyObject *error_callback = rf->error_callback;
						if(error_callback) {
							bool was_unblocked = rf_block_threads(rf);
							PyObject *args = Py_BuildValue("y#i",
								message, multiplex_in_remaining, channel);
							free(message);
							if(!args)
								return RF_STATUS_PYTHON;
							PyObject *result = PyObject_CallObject(error_callback, args);
							Py_DecRef(args);
							if(!result)
								return RF_STATUS_PYTHON;
							Py_DecRef(result);
							if(was_unblocked)
								rf_unblock_threads(rf);
						} else {
//							fprintf(stderr, "<%d> ", channel);
							fwrite(message, sizeof *message, multiplex_in_remaining, stderr);
							free(message);
						}
					}
					break;
				case MSG_IO_ERROR:
				case MSG_SUCCESS:;
					int32_t err;
					if(multiplex_in_remaining != sizeof err)
						return RF_STATUS_PROTO;
					RF_PROPAGATE_ERROR(rf_recv_bytes_raw(rf, (char *)&err, sizeof err));
					break;
				case MSG_NO_SEND:
					if(multiplex_in_remaining != sizeof err)
						return RF_STATUS_PROTO;
					RF_PROPAGATE_ERROR(rf_recv_bytes_raw(rf, (char *)&err, sizeof err));
					rf_flist_entry_t *entry = rf_find_ndx(rf, le32(err));
					if(entry) {
						PyObject *data_callback = entry->data_callback;
						if(data_callback) {
							bool was_unblocked = rf_block_threads(rf);
							PyObject *args = PyTuple_New(0);
							if(!args)
								return RF_STATUS_PYTHON;
							PyObject *result = PyObject_CallObject(data_callback, args);
							Py_DecRef(args);
							entry->data_callback = NULL;
							Py_DecRef(data_callback);
							if(!result)
								return RF_STATUS_PYTHON;
							Py_DecRef(result);
							if(was_unblocked)
								rf_unblock_threads(rf);
						}
					}
					break;
				case MSG_NOOP:
					if(multiplex_in_remaining)
						return RF_STATUS_PROTO;
					break;
				default:
					return RF_STATUS_PROTO;
			}
		}
	}
}

__attribute__((hot))
static rf_status_t rf_send_bytes_raw(RsyncFetch_t *rf, char *src, size_t len) {
	rf_pipestream_t *stream = &rf->out_stream;
	size_t fill = stream->fill;
	size_t size = stream->size;
	size_t offset = stream->offset;
	char *buf = stream->buf;

	if(buf) {
		if(fill + len > size) {
			size_t newsize = (size + RF_BUFSIZE_ADJUSTMENT) << 1;
			if(newsize < RF_STREAM_OUT_BUFSIZE)
				newsize = RF_STREAM_OUT_BUFSIZE;
			while(fill + len > newsize - RF_BUFSIZE_ADJUSTMENT)
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
		if(size < RF_STREAM_OUT_BUFSIZE)
			size = RF_STREAM_OUT_BUFSIZE;
		while(len > size - RF_BUFSIZE_ADJUSTMENT)
			size <<= 1;
		size -= RF_BUFSIZE_ADJUSTMENT;
		buf = malloc(size);
		if(!buf)
			return RF_STATUS_ERRNO;
		stream->buf = buf;
		stream->size = size;
	}

	size_t start = offset + fill;
	if(start > size)
		start -= size;

	if(len == 1) {
		buf[start] = *src;
	} if(start + len > size) {
		size_t amount = size - start;
		memcpy(buf + start, src, amount);
		memcpy(buf, src + amount, len - amount);
	} else {
		memcpy(buf + start, src, len);
	}

	stream->fill = fill + len;

	return RF_STATUS_OK;
}

__attribute__((hot))
static rf_status_t rf_send_bytes(RsyncFetch_t *rf, char *buf, size_t len) {
	if(!rf->multiplex) {
		RF_PROPAGATE_ERROR(rf_flush_output(rf));
		return rf_send_bytes_raw(rf, buf, len);
	}
	size_t multiplex_out_remaining = rf->multiplex_out_remaining;
	if(multiplex_out_remaining + len >= 0xFFFFFF) {
		size_t chunk = 0xFFFFFF - multiplex_out_remaining;
		RF_PROPAGATE_ERROR(rf_send_bytes_raw(rf, buf, chunk));
		rf->multiplex_out_remaining = 0xFFFFFF;
		RF_PROPAGATE_ERROR(rf_flush_output(rf));

		buf += chunk;
		len -= chunk;

		while(len >= 0xFFFFFF) {
			uint8_t mplex[4] = { 0xFF, 0xFF, 0xFF, MSG_DATA + MPLEX_BASE };
			RF_PROPAGATE_ERROR(rf_send_bytes_raw(rf, (char *)mplex, sizeof mplex));
			RF_PROPAGATE_ERROR(rf_send_bytes_raw(rf, buf, 0xFFFFFF));
			len -= 0xFFFFFF;
			buf += 0xFFFFFF;
		}

		multiplex_out_remaining = 0;
	}		

	if(!len)
		return RF_STATUS_OK;

	if(!multiplex_out_remaining) {
		uint8_t mplex[4] = { 0, 0, 0, MSG_DATA + MPLEX_BASE };
		RF_PROPAGATE_ERROR(rf_send_bytes_raw(rf, (char *)mplex, sizeof mplex));
	}

	RF_PROPAGATE_ERROR(rf_send_bytes_raw(rf, buf, len));
	rf->multiplex_out_remaining = multiplex_out_remaining + len;

	return RF_STATUS_OK;
}

__attribute__((unused))
static inline rf_status_t rf_send_int8(RsyncFetch_t *rf, int8_t d) {
	return rf_send_bytes(rf, (char *)&d, sizeof d);
}

static inline rf_status_t rf_send_uint8(RsyncFetch_t *rf, uint8_t d) {
	return rf_send_bytes(rf, (char *)&d, sizeof d);
}

__attribute__((unused))
static inline rf_status_t rf_send_int16(RsyncFetch_t *rf, int16_t d) {
	int16_t le = le16(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

static inline rf_status_t rf_send_uint16(RsyncFetch_t *rf, uint16_t d) {
	uint16_t le = le16(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static inline rf_status_t rf_send_int32(RsyncFetch_t *rf, int32_t d) {
	int32_t le = le32(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

static inline rf_status_t rf_send_uint32(RsyncFetch_t *rf, uint32_t d) {
	uint32_t le = le32(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static inline rf_status_t rf_send_int64(RsyncFetch_t *rf, int64_t d) {
	int64_t le = le64(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static inline rf_status_t rf_send_uint64(RsyncFetch_t *rf, uint64_t d) {
	uint64_t le = le64(d);
	return rf_send_bytes(rf, (char *)&le, sizeof le);
}

__attribute__((unused))
static inline rf_status_t rf_recv_int8(RsyncFetch_t *rf, int8_t *d) {
	return rf_recv_bytes(rf, (char *)d, sizeof *d);
}

static inline rf_status_t rf_recv_uint8(RsyncFetch_t *rf, uint8_t *d) {
	return rf_recv_bytes(rf, (char *)d, sizeof *d);
}

__attribute__((unused))
static inline rf_status_t rf_recv_int16(RsyncFetch_t *rf, int16_t *d) {
	int16_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le16(le);
	return RF_STATUS_OK;
}

static inline rf_status_t rf_recv_uint16(RsyncFetch_t *rf, uint16_t *d) {
	uint16_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le16(le);
	return RF_STATUS_OK;
}

static inline rf_status_t rf_recv_int32(RsyncFetch_t *rf, int32_t *d) {
	int32_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le32(le);
	return RF_STATUS_OK;
}

static inline rf_status_t rf_recv_uint32(RsyncFetch_t *rf, uint32_t *d) {
	uint32_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le32(le);
	return RF_STATUS_OK;
}

__attribute__((unused))
static inline rf_status_t rf_recv_int64(RsyncFetch_t *rf, int64_t *d) {
	int64_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le64(le);
	return RF_STATUS_OK;
}

__attribute__((unused))
static inline rf_status_t rf_recv_uint64(RsyncFetch_t *rf, uint64_t *d) {
	uint64_t le;
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)&le, sizeof le));
	*d = le64(le);
	return RF_STATUS_OK;
}

static const uint8_t rf_varint_extra[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // (00 - 3F)/4
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // (40 - 7F)/4
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // (80 - BF)/4
	2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 5, 6, // (C0 - FF)/4
};

static rf_status_t rf_recv_varint(RsyncFetch_t *rf, int32_t *d) {
	uint8_t init;
	RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &init));
	size_t extra = rf_varint_extra[init >> 2];
	if(extra) {
		uint8_t extra_bytes[7];
		RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)extra_bytes, extra));
		extra_bytes[extra] = init & ((1 << (8 - extra)) - 1);
		int32_t v = 0;
		for(int i = 0; i <= extra; i++)
			v |= extra_bytes[i] << (8 * i);
		*d = v;
	} else {
		*d = init;
	}
	return RF_STATUS_OK;
}

static rf_status_t rf_recv_varlong(RsyncFetch_t *rf, size_t min_bytes, int64_t *d) {
	if(min_bytes > 8)
		return RF_STATUS_ASSERT;
	uint8_t init_bytes[9] = {0};
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)init_bytes, min_bytes));
	size_t extra = rf_varint_extra[init_bytes[0] >> 2];
	uint8_t *extra_bytes = init_bytes + 1;
	size_t total_bytes = min_bytes + extra;
	if(extra) {
		if(total_bytes >= sizeof(int64_t))
			return RF_STATUS_PROTO;
		RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)extra_bytes + min_bytes, extra));
	}
	extra_bytes[total_bytes] = init_bytes[0] & ((1 << (8 - extra)) - 1);
	int64_t v = 0;
	for(int i = 0; i <= total_bytes; i++)
		v |= extra_bytes[i] << (8 * i);
	*d = v;
	return RF_STATUS_OK;
}

static rf_status_t rf_recv_ndx(RsyncFetch_t *rf, int32_t *d) {
	int32_t *prev_ptr;
	bool is_positive;
	uint8_t init;
	RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &init));
	if(init == UINT8_C(0xFF)) {
		RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &init));
		prev_ptr = &rf->prev_negative_ndx_in;
		is_positive = false;
	} else if(init) {
		prev_ptr = &rf->prev_positive_ndx_in;
		is_positive = true;
	} else {
		return *d = NDX_DONE, RF_STATUS_OK;
	}

	int32_t ndx;
	if(init == 0xFE) {
		RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &init));
		if(init & 0x80) {
			uint8_t extra_bytes[4];
			RF_PROPAGATE_ERROR(rf_recv_bytes(rf, (char *)extra_bytes, sizeof extra_bytes - 1));
			extra_bytes[sizeof extra_bytes - 1] = init & 0x7F;
			ndx = 0;
			for(int i = 0; i < sizeof extra_bytes; i++)
				ndx |= extra_bytes[i] << (8 * i);
		} else {
			uint8_t onemorebyte;
			RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &onemorebyte));
			ndx = ((init << 8) | onemorebyte) + *prev_ptr;
		}
	} else {
		ndx = init + *prev_ptr;
	}

	*prev_ptr = ndx;
	*d = is_positive ? ndx : -ndx;
	return RF_STATUS_OK;
}

static rf_status_t rf_send_ndx(RsyncFetch_t *rf, int32_t ndx) {
	int32_t diff;
	if(ndx == NDX_DONE) {
		return rf_send_uint8(rf, 0);
	} else if(ndx >= 0) {
		diff = ndx - rf->prev_positive_ndx_out;
		rf->prev_positive_ndx_out = ndx;
	} else {
		RF_PROPAGATE_ERROR(rf_send_uint8(rf, UINT8_C(0xFF)));
		ndx = -ndx;
		diff = ndx - rf->prev_negative_ndx_out;
		rf->prev_negative_ndx_out = ndx;
	}

	if(diff < 0xFE && diff > 0) {
		return rf_send_uint8(rf, diff);
	} else {
		RF_PROPAGATE_ERROR(rf_send_uint8(rf, UINT8_C(0xFE)));
		if(diff < 0 || diff > 0x7FFFF) {
			RF_PROPAGATE_ERROR(rf_send_uint8(rf, (ndx >> 24) | UINT8_C(0x80)));
			RF_PROPAGATE_ERROR(rf_send_uint8(rf, ndx));
			RF_PROPAGATE_ERROR(rf_send_uint8(rf, ndx >> 8));
			return rf_send_uint8(rf, ndx >> 16);
		} else {
			RF_PROPAGATE_ERROR(rf_send_uint8(rf, diff >> 8));
			return rf_send_uint8(rf, diff);
		}
	}
}

static rf_status_t rf_recv_vstring(RsyncFetch_t *rf, char **bufp) {
	RF_PROPAGATE_ERROR(rf_refstring_free(rf, bufp));
	uint8_t b;
	RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &b));
	size_t len;
	if(b & 0x80) {
		len = (b & 0x7F) << 8;
		RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &b));
		len |= b;
	} else {
		len = b;
	}
	char *buf;
	RF_PROPAGATE_ERROR(rf_refstring_newlen(rf, NULL, len, &buf));
	rf_status_t s = rf_recv_bytes(rf, buf, len);
	if(s == RF_STATUS_OK)
		*bufp = buf;
	else
		rf_refstring_free(rf, &buf);
	return s;
}

static void rf_flist_entry_clear(RsyncFetch_t *rf, rf_flist_entry_t *entry) {
	rf_refstring_free(rf, &entry->name);
	rf_refstring_free(rf, &entry->user);
	rf_refstring_free(rf, &entry->group);
	rf_refstring_free(rf, &entry->symlink);
	rf_refstring_free(rf, &entry->hardlink);
	PyObject *data_callback = entry->data_callback;
	if(data_callback) {
		rf_block_threads(rf);
		entry->data_callback = NULL;
		Py_DecRef(data_callback);
	}
}

static void rf_flist_entry_free(RsyncFetch_t *rf, rf_flist_entry_t *entry) {
	rf_flist_entry_clear(rf, entry);
	free(entry);
}

static int memcmp2(const void *a_buf, const void *b_buf, size_t a_len, size_t b_len) {
	if(a_len < b_len) {
		int c = memcmp(a_buf, b_buf, a_len);
		if(c == 0)
			return -1;
		else
			return c;
	} else {
		int c = memcmp(a_buf, b_buf, b_len);
		if(c == 0)
			return a_len != b_len;
		else
			return c;
	}

}

static int rf_flist_entry_cmp(const void *ap, const void *bp) {
	const rf_flist_entry_t *a = *(const rf_flist_entry_t **)ap;
	const rf_flist_entry_t *b = *(const rf_flist_entry_t **)bp;

	bool a_isdir = S_ISDIR(a->mode);
	bool b_isdir = S_ISDIR(b->mode);

	char *a_name = a->name;
	char *b_name = b->name;

	size_t a_namelen = rf_refstring_len(a_name);
	size_t b_namelen = rf_refstring_len(b_name);

	char *a_basename = memrchr(a_name, '/', a_namelen);
	char *b_basename = memrchr(b_name, '/', b_namelen);

	size_t a_basename_len, b_basename_len;
	size_t a_dirname_len, b_dirname_len;

	if(a_basename) {
		a_basename++;
		a_dirname_len = a_basename - a_name;
		a_basename_len = a_namelen - a_dirname_len;
	} else {
		a_basename = a_name;
		a_dirname_len = 0;
		a_basename_len = a_namelen;
	}

	if(b_basename) {
		b_basename++;
		b_dirname_len = b_basename - b_name;
		b_basename_len = b_namelen - b_dirname_len;
	} else {
		b_basename = b_name;
		b_dirname_len = 0;
		b_basename_len = b_namelen;
	}

	if(a_dirname_len == b_dirname_len && memcmp(a_name, b_name, a_dirname_len) == 0) {
		if(a_isdir) {
			if(b_isdir) {
				if(a_basename_len == 1 && a_basename[0] == '.'
				&& !(b_basename_len == 1 && b_basename[0] == '.'))
					return -1;
			} else {
				if(a_basename_len == 1 && a_basename[0] == '.')
					return -1;
				else
					return 1;
			}
		} else {
			if(b_basename_len == 1 && b_basename[0] == '.')
				return 1;
			if(b_isdir)
				return -1;
		}
		return memcmp2(a_basename, b_basename, a_basename_len, b_basename_len);
	}

	// if b is an ancestor of a
	if((!a_isdir || (a_basename_len == 1 && a_basename[0] == '.'))
	&& (b_dirname_len >= a_dirname_len && memcmp(a_name, b_name, a_dirname_len) == 0))
		return -1;

	// if a is an ancestor of b
	if((!b_isdir || (b_basename_len == 1 && b_basename[0] == '.'))
	&& (a_dirname_len >= b_dirname_len && memcmp(b_name, a_name, b_dirname_len) == 0))
		return 1;

	return memcmp2(a_name, b_name, a_namelen, b_namelen);
}

static rf_status_t rf_flist_new(RsyncFetch_t *rf, int32_t offset, rf_flist_t **flistp) {
	rf_flist_t *flist = malloc(sizeof *flist);
	if(!flist)
		return RF_STATUS_ERRNO;
	*flist = rf_flist_0;
	flist->offset = offset;
	rf_flist_t *tail = rf->flists_tail;
	if(tail)
		tail->next = flist;
	else
		rf->flists_head = flist;
	rf->flists_tail = flist;
	flist->prev = tail;
	*flistp = flist;
	return RF_STATUS_OK;
}

static void rf_flist_free(RsyncFetch_t *rf, rf_flist_t **flistp) {
	if(flistp) {
		rf_flist_t *flist = *flistp;
		if(flist) {
			bool threads_unblocked = (bool)rf->py_thread_state;
			rf_flist_t *next = flist->next;
			rf_flist_t *prev = flist->prev;
			if(next)
				next->prev = prev;
			else
				rf->flists_tail = prev;
			if(prev)
				prev->next = next;
			else
				rf->flists_head = next;
			rf_flist_entry_t **entries = flist->entries;
			if(entries) {
				size_t num = flist->num;
				for(size_t i = 0; i < num; i++)
					rf_flist_entry_free(rf, entries[i]);
				free(entries);
			}
			free(flist);
			if(threads_unblocked)
				rf_unblock_threads(rf);
		}
		*flistp = NULL;
	}
}

static rf_status_t rf_flist_add_entry(RsyncFetch_t *rf, rf_flist_t *flist, rf_flist_entry_t *entry) {
	size_t num = flist->num;
	size_t size = flist->size;
	rf_flist_entry_t **entries = flist->entries;
	if(num == size) {
		size += RF_BUFNUM_ADJUSTMENT;
		if(size < 16)
			size = 16;
		while(size < RF_BUFNUM_ADJUSTMENT || size - RF_BUFNUM_ADJUSTMENT <= num)
			size <<= 1;
		size -= RF_BUFNUM_ADJUSTMENT;
		entries = realloc(entries, size * sizeof *entries);
		if(!entries)
			return RF_STATUS_ERRNO;
		flist->entries = entries;
		flist->size = size;
	}
	entries[num] = entry;
	flist->num = num + 1;
	
	return RF_STATUS_OK;
}

static rf_status_t rf_flist_sort(RsyncFetch_t *rf, rf_flist_t *flist) {
	rf_flist_entry_t **entries = flist->entries;
	if(entries)
		qsort(entries, flist->num, sizeof *entries, rf_flist_entry_cmp);
	return RF_STATUS_OK;
}

static rf_status_t rf_fill_flist_entry(RsyncFetch_t *rf, rf_flist_entry_t *entry, uint16_t xflags) {
	uint8_t b;
	char *last_name = rf->last.name;
	size_t len1;
	if(xflags & XMIT_SAME_NAME) {
		RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &b));
		len1 = b;
		if(len1 > rf_refstring_len(last_name))
			return RF_STATUS_PROTO;
	} else {
		len1 = 0;
	}

	size_t len2;
	if(xflags & XMIT_LONG_NAME) {
		int32_t s32;
		RF_PROPAGATE_ERROR(rf_recv_varint(rf, &s32));
		len2 = s32;
	} else {
		RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &b));
		len2 = b;
	}

	size_t name_len = len1 + len2;
	if(!name_len)
		return RF_STATUS_PROTO;

	char *name;
	RF_PROPAGATE_ERROR(rf_refstring_newlen(rf, NULL, name_len, &name));
	entry->name = name;

	if(len1)
		memcpy(name, last_name, len1);
	RF_PROPAGATE_ERROR(rf_recv_bytes(rf, name + len1, len2));
	if(memchr(name, '\0', name_len))
		return RF_STATUS_PROTO;

	RF_PROPAGATE_ERROR(rf_refstring_dup(rf, name, &rf->last.name));

	entry->ndx = rf->ndx++;

	rf_flist_entry_t *hardlink;
	if(xflags & XMIT_HLINKED && !(xflags & XMIT_HLINK_FIRST)) {
		int32_t hlink;
		RF_PROPAGATE_ERROR(rf_recv_varint(rf, &hlink));

		rf_flist_t *flist = rf->flist;
		if(hlink < flist->offset) {
			hardlink = rf_find_ndx(rf, hlink);
			RF_PROPAGATE_ERROR(rf_refstring_dup(rf, hardlink->name, &entry->hardlink));
		} else {
			hardlink = rf_flist_get_entry(rf, flist, hlink);

			entry->size = hardlink->size;
			uint32_t mode = entry->mode = hardlink->mode;
			uint64_t mtime = entry->mtime = hardlink->mtime;
			uint32_t uid = entry->uid = hardlink->uid;
			uint32_t gid = entry->gid = hardlink->gid;

			char *user = hardlink->user;
			RF_PROPAGATE_ERROR(rf_refstring_dup(rf, user, &entry->user));

			char *group = hardlink->group;
			RF_PROPAGATE_ERROR(rf_refstring_dup(rf, group, &entry->group));

			rf->last.mode = mode;
			rf->last.mtime = mtime;
			rf->last.uid = uid;
			rf->last.gid = gid;
			RF_PROPAGATE_ERROR(rf_refstring_dup(rf, user, &rf->last.user));
			RF_PROPAGATE_ERROR(rf_refstring_dup(rf, group, &rf->last.group));

			RF_PROPAGATE_ERROR(rf_refstring_dup(rf, hardlink->name, &entry->hardlink));
			return RF_STATUS_OK;
		}
	} else {
		hardlink = NULL;
	}

	RF_PROPAGATE_ERROR(rf_recv_varlong(rf, 3, &entry->size));

	if(xflags & XMIT_SAME_TIME) {
		entry->mtime = rf->last.mtime;
	} else {
		int64_t mtime;
		RF_PROPAGATE_ERROR(rf_recv_varlong(rf, 4, &mtime));
		rf->last.mtime = entry->mtime = mtime;
	}

	int32_t mode;
	if(xflags & XMIT_SAME_MODE) {
		mode = entry->mode = rf->last.mode;
	} else {
		RF_PROPAGATE_ERROR(rf_recv_int32(rf, &mode));
		rf->last.mode = entry->mode = mode;
	}

	if(xflags & XMIT_SAME_UID) {
		entry->uid = rf->last.uid;
		RF_PROPAGATE_ERROR(rf_refstring_dup(rf, rf->last.user, &entry->user));
	} else {
		RF_PROPAGATE_ERROR(rf_recv_varint(rf, &entry->uid));
		if(xflags & XMIT_USER_NAME_FOLLOWS) {
			uint8_t len;
			RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &len));
			char *user;
			RF_PROPAGATE_ERROR(rf_refstring_newlen(rf, NULL, len, &user));
			entry->user = user;
			RF_PROPAGATE_ERROR(rf_recv_bytes(rf, user, len));
			RF_PROPAGATE_ERROR(rf_refstring_dup(rf, user, &rf->last.user));
		}
	}

	if(xflags & XMIT_SAME_GID) {
		entry->gid = rf->last.gid;
		RF_PROPAGATE_ERROR(rf_refstring_dup(rf, rf->last.group, &entry->group));
	} else {
		RF_PROPAGATE_ERROR(rf_recv_varint(rf, &entry->gid));
		if(xflags & XMIT_GROUP_NAME_FOLLOWS) {
			uint8_t len;
			RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &len));
			char *group;
			RF_PROPAGATE_ERROR(rf_refstring_newlen(rf, NULL, len, &group));
			entry->group = group;
			RF_PROPAGATE_ERROR(rf_recv_bytes(rf, group, len));
			RF_PROPAGATE_ERROR(rf_refstring_dup(rf, group, &rf->last.group));
		}
	}

	if(S_ISCHR(mode) || S_ISBLK(mode) || S_ISFIFO(mode) || S_ISSOCK(mode)) {
		int32_t major;
		if(xflags & XMIT_SAME_RDEV_MAJOR) {
			major = entry->major = rf->last.major;
		} else {
			RF_PROPAGATE_ERROR(rf_recv_varint(rf, &major));
			entry->major = rf->last.major = major;
		}
		RF_PROPAGATE_ERROR(rf_recv_varint(rf, &entry->minor));
	}

	if(S_ISLNK(mode)) {
		int32_t len;
		RF_PROPAGATE_ERROR(rf_recv_varint(rf, &len));
		if(len > 65536)
			return RF_STATUS_PROTO;
		char *symlink;
		RF_PROPAGATE_ERROR(rf_refstring_newlen(rf, NULL, len, &symlink));
		entry->symlink = symlink;
		RF_PROPAGATE_ERROR(rf_recv_bytes(rf, symlink, len));
	}

	return RF_STATUS_OK;
}

static rf_status_t rf_recv_flist_entry(RsyncFetch_t *rf, rf_flist_entry_t **entryp) {
	uint8_t b;
	RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &b));
	if(!b)
		return *entryp = NULL, RF_STATUS_OK;

	uint16_t xflags = b;
	if(xflags & XMIT_EXTENDED_FLAGS) {
		RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &b));
		xflags |= (uint16_t)b << 8;
	}

	rf_flist_entry_t *entry = malloc(sizeof *entry);
	if(!entry)
		return RF_STATUS_ERRNO;
	*entry = rf_flist_entry_0;

	rf_status_t s = rf_fill_flist_entry(rf, entry, xflags);
	if(s != RF_STATUS_OK) {
		rf_flist_entry_free(rf, entry);
		return s;
	}

	return *entryp = entry, RF_STATUS_OK;
}

static rf_status_t rf_recv_flist(RsyncFetch_t *rf) {
	rf_flist_t *flist;
	RF_PROPAGATE_ERROR(rf_flist_new(rf, rf->ndx, &flist));
	rf->flist = flist;
	for(;;) {
		rf_flist_entry_t *entry;
		RF_PROPAGATE_ERROR(rf_recv_flist_entry(rf, &entry));
		if(!entry)
			break;
		RF_PROPAGATE_ERROR(rf_flist_add_entry(rf, flist, entry));
	}
	RF_PROPAGATE_ERROR(rf_flist_sort(rf, flist));
	rf->ndx++;
	size_t num = flist->num;
	size_t offset = flist->offset;
	rf_flist_entry_t **entries = flist->entries;
	for(size_t i = 0; i < num; i++) {
		rf_flist_entry_t *entry = entries[i];

		rf_block_threads(rf);
		PyObject *args = Py_BuildValue("y#LLLLy#Ly#LLy#y#",
			entry->name, rf_refstring_len(entry->name),
			(long long)entry->size,
			(long long)entry->mtime,
			(long long)entry->mode,
			(long long)entry->uid,
			entry->user, rf_refstring_len(entry->user),
			(long long)entry->gid,
			entry->group, rf_refstring_len(entry->group),
			(long long)entry->major,
			(long long)entry->minor,
			entry->symlink, rf_refstring_len(entry->symlink),
			entry->hardlink, rf_refstring_len(entry->hardlink)
		);
		if(!args)
			return RF_STATUS_PYTHON;
		PyObject *data_callback = PyObject_CallObject(rf->entry_callback, args);
		Py_DecRef(args);
		if(!data_callback)
			return RF_STATUS_PYTHON;
		if(data_callback == Py_None) {
			Py_DecRef(data_callback);
		} else {
			entry->data_callback = data_callback;
			if(S_ISREG(entry->mode) && !entry->hardlink) {
				RF_PROPAGATE_ERROR(rf_send_ndx(rf, offset + i));
				RF_PROPAGATE_ERROR(rf_send_uint16(rf, ITEM_TRANSFER));
				RF_PROPAGATE_ERROR(rf_send_uint32(rf, 0)); // number of checksums
				RF_PROPAGATE_ERROR(rf_send_uint32(rf, MAX_BLOCK_SIZE)); // block length
				RF_PROPAGATE_ERROR(rf_send_uint32(rf, 2)); // checksum length
				RF_PROPAGATE_ERROR(rf_send_uint32(rf, 0)); // remainder length
			} else {
				PyObject *args = PyTuple_New(0);
				if(!args)
					return RF_STATUS_PYTHON;
				PyObject *result = PyObject_CallObject(data_callback, args);
				Py_DecRef(args);
				entry->data_callback = NULL;
				Py_DecRef(data_callback);
				if(!result)
					return RF_STATUS_PYTHON;
				Py_DecRef(result);
			}
		}
	}
	rf_unblock_threads(rf);
	RF_PROPAGATE_ERROR(rf_send_ndx(rf, NDX_DONE));
	return RF_STATUS_OK;
}

static rf_status_t rf_recv_filedata(RsyncFetch_t *rf, rf_flist_entry_t *entry) {
	PyObject *data_callback = entry->data_callback;
	if(!data_callback)
		return RF_STATUS_PROTO;

	PyObject *bytes = rf->chunk_bytes;
	size_t size = rf->chunk_size;
	size_t fill = 0;
	uint32_t remaining = 0;
	char *buf = rf->chunk_buffer;
	if(!bytes || !buf)
		return RF_STATUS_ASSERT;

	for(;;) {
		if(!remaining) {
			RF_PROPAGATE_ERROR(rf_recv_uint32(rf, &remaining));
			if(!remaining)
				break;
		}

		size_t avail = size - fill;
		if(remaining < avail) {
			RF_PROPAGATE_ERROR(rf_recv_bytes(rf, buf + fill, remaining));
			fill += remaining;
			remaining = 0;
		} else {
			RF_PROPAGATE_ERROR(rf_recv_bytes(rf, buf + fill, avail));
			fill = 0; // filled to the brim, actually, but not for long
			remaining -= avail;

			rf_block_threads(rf);

			PyObject *args = PyTuple_Pack(1, bytes);
			if(!args)
				return RF_STATUS_PYTHON;
			// now owned by the args tuple
			rf->chunk_bytes = NULL;

			PyObject *result = PyObject_CallObject(data_callback, args);
			Py_DecRef(args);
			if(!result)
				return RF_STATUS_PYTHON;
			Py_DecRef(result);

			// allocate a new buffer for the next chunk (while we still hold the GIL)
			bytes = PyBytes_FromStringAndSize(NULL, size);
			if(!bytes)
				return RF_STATUS_PYTHON;
			rf->chunk_bytes = bytes;
			rf->chunk_buffer = PyBytes_AS_STRING(bytes);
			
			rf_unblock_threads(rf);
		}
	}

	rf_block_threads(rf);

	if(fill) {
		if(_PyBytes_Resize(&bytes, fill) == -1) {
			// if this fails the old copy is gone too
			rf->chunk_bytes = NULL;
			return RF_STATUS_PYTHON;
		}
		PyObject *args = PyTuple_Pack(1, bytes);
		rf->chunk_bytes = NULL;
		if(!args) {
			Py_DecRef(bytes);
			return RF_STATUS_PYTHON;
		}
		// now owned by the args tuple
		rf->chunk_bytes = NULL;

		PyObject *result = PyObject_CallObject(data_callback, args);
		Py_DecRef(args);
		if(!result)
			return RF_STATUS_PYTHON;
		Py_DecRef(result);

		// allocate a new buffer for the next chunk (while we still hold the GIL)
		bytes = PyBytes_FromStringAndSize(NULL, size);
		if(!bytes)
			return RF_STATUS_PYTHON;
		rf->chunk_bytes = bytes;
		rf->chunk_buffer = PyBytes_AS_STRING(bytes);
	}

	PyObject *args = PyTuple_New(0);
	if(!args)
		return RF_STATUS_PYTHON;
	PyObject *result = PyObject_CallObject(data_callback, args);
	Py_DecRef(args);
	entry->data_callback = NULL;
	Py_DecRef(data_callback);
	if(!result)
		return RF_STATUS_PYTHON;
	Py_DecRef(result);

	rf_unblock_threads(rf);

	return RF_STATUS_OK;
}

static rf_status_t rf_mainloop(RsyncFetch_t *rf) {
	uint32_t remote_protocol;
	RF_PROPAGATE_ERROR(rf_recv_uint32(rf, &remote_protocol));
	if(remote_protocol < 30)
		return RF_STATUS_PROTO;
	RF_PROPAGATE_ERROR(rf_send_uint32(rf, 30));

	uint8_t cflags;
	RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &cflags));

	uint32_t checksum_seed;
	RF_PROPAGATE_ERROR(rf_recv_uint32(rf, &checksum_seed));

	rf->multiplex = true;

	char **filters = rf->filters;
	if(filters) {
		size_t num = rf->filters_num;
		for(size_t i = 0; i < num; i++) {
			char *filter = filters[i];
			size_t filter_len = filters[i + 1] - filter - 1;
			RF_PROPAGATE_ERROR(rf_send_uint32(rf, filter_len));
			RF_PROPAGATE_ERROR(rf_send_bytes(rf, filter, filter_len));
		}
	}

	RF_PROPAGATE_ERROR(rf_send_uint32(rf, 0));

	RF_PROPAGATE_ERROR(rf_recv_flist(rf));

	int phase = 0;

	for(;;) {
		int32_t ndx;
		RF_PROPAGATE_ERROR(rf_recv_ndx(rf, &ndx));
		if(ndx == NDX_FLIST_EOF) {
			// do nothing
		} else if(ndx == NDX_DONE) {
			rf_flist_t *flist = rf->flists_head;
			rf_flist_free(rf, &flist);
			if(!rf->flists_head) {
				phase++;
				RF_PROPAGATE_ERROR(rf_send_ndx(rf, NDX_DONE));
			}
			if(phase > 2)
				break;
		} else if(ndx > 0) {
			// recv_attrs
			uint16_t iflags;
			RF_PROPAGATE_ERROR(rf_recv_uint16(rf, &iflags));
			uint8_t b;
			if(iflags & ITEM_BASIS_TYPE_FOLLOWS)
				RF_PROPAGATE_ERROR(rf_recv_uint8(rf, &b));
			if(iflags & ITEM_XNAME_FOLLOWS) {
				char *refname;
				RF_PROPAGATE_ERROR(rf_recv_vstring(rf, &refname));
				rf_refstring_free(rf, &refname);
			}

			// recv_sum_head
			uint32_t number_of_checksums;
			RF_PROPAGATE_ERROR(rf_recv_uint32(rf, &number_of_checksums));
			uint32_t block_length;
			RF_PROPAGATE_ERROR(rf_recv_uint32(rf, &block_length));
			uint32_t checksum_length;
			RF_PROPAGATE_ERROR(rf_recv_uint32(rf, &checksum_length));
			uint32_t remainder_length;
			RF_PROPAGATE_ERROR(rf_recv_uint32(rf, &remainder_length));

			if(number_of_checksums)
				return RF_STATUS_PROTO;

			RF_PROPAGATE_ERROR(rf_recv_filedata(rf, rf_find_ndx(rf, ndx)));

			char md5[16];
			RF_PROPAGATE_ERROR(rf_recv_bytes(rf, md5, sizeof md5));
		} else {
			// ndx = NDX_FLIST_OFFSET - ndx
			RF_PROPAGATE_ERROR(rf_recv_flist(rf));
		}
	}

	// stats follow

	int64_t total_read;
	RF_PROPAGATE_ERROR(rf_recv_varlong(rf, 3, &total_read));
	int64_t total_written;
	RF_PROPAGATE_ERROR(rf_recv_varlong(rf, 3, &total_written));
	int64_t total_size;
	RF_PROPAGATE_ERROR(rf_recv_varlong(rf, 3, &total_size));
	int64_t flist_buildtime;
	RF_PROPAGATE_ERROR(rf_recv_varlong(rf, 3, &flist_buildtime));
	int64_t flist_xfertime;
	RF_PROPAGATE_ERROR(rf_recv_varlong(rf, 3, &flist_xfertime));

	RF_PROPAGATE_ERROR(rf_send_ndx(rf, NDX_DONE));

	return rf_wait_for_eof(rf);
}

static rf_status_t rf_run(RsyncFetch_t *rf) {
	int in_pipe[2], out_pipe[2], err_pipe[2];
	if(create_pipe(in_pipe) != -1) {
		rf->in_stream.fd = in_pipe[0];
		if(create_pipe(out_pipe) != -1) {
			rf->out_stream.fd = out_pipe[1];
			if(create_pipe(err_pipe) != -1) {
				rf->err_stream.fd = err_pipe[0];
				int pid = vfork();
				if(pid == 0) {
					if(dup2(out_pipe[0], STDIN_FILENO) == -1
					|| dup2(in_pipe[1], STDOUT_FILENO) == -1
					|| dup2(err_pipe[1], STDERR_FILENO) == -1
					) {
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

					char **command = rf->command;
					execvp(command[0], command);
					perror("execvp");
					_exit(2);
				} else if(pid != -1) {
					rf->pid = pid;
					if(close(in_pipe[1]) != -1) {
						in_pipe[1] = -1;
						if(close(out_pipe[0]) != -1) {
							out_pipe[0] = -1;
							if(close(err_pipe[1]) != -1) {
								err_pipe[1] = -1;
								return rf_mainloop(rf);
							}
						}
					}
				}
				if(err_pipe[1] != -1)
					close(err_pipe[1]);
			}
			if(out_pipe[0] != -1)
				close(out_pipe[0]);
		}
		if(in_pipe[1] != -1)
			close(in_pipe[1]);
	}

	return RF_STATUS_ERRNO;
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
			return PyErr_Format(PyExc_RuntimeError, "rsync process exited prematurely");
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

static void rf_stream_clear(RsyncFetch_t *rf, rf_pipestream_t *stream) {
	if(stream->fd != -1) {
		close(stream->fd);
		stream->fd = -1;
	}
	free(stream->buf);
	stream->buf = NULL;
}

static void rf_clear(RsyncFetch_t *rf) {
	if(rf) {
		rf->closed = true;

		rf_stream_clear(rf, &rf->in_stream);
		rf_stream_clear(rf, &rf->out_stream);
		rf_stream_clear(rf, &rf->err_stream);

		if(rf->pid) {
			while(waitpid(rf->pid, NULL, 0) == -1 && errno == EINTR);
			rf->pid = 0;
		}

		rf_flist_entry_clear(rf, &rf->last);

		for(;;) {
			rf_flist_t *flist = rf->flists_head;
			if(!flist)
				break;
			rf_flist_free(rf, &flist);
		}

		free(rf->command);
		rf->command = NULL;
		free(rf->filters);
		rf->filters = NULL;

		rf_block_threads(rf);
		Py_CLEAR(rf->entry_callback);
		Py_CLEAR(rf->error_callback);
		Py_CLEAR(rf->chunk_bytes);
	}
}

static int RsyncFetch_dealloc(PyObject *self) {
	RsyncFetch_t *rf = RsyncFetch_Check(self, false);

	if(rf) {
		rf->magic = 0;
		rf_clear(rf);
		PyThread_free_lock(rf->lock);
	}

	freefunc tp_free = Py_TYPE(self)->tp_free ?: PyObject_Free;
	tp_free(self);

	return 0;
}

static PyObject *RsyncFetch_close_locked(RsyncFetch_t *rf) {
	if(rf->closed)
		return PyErr_Format(PyExc_RuntimeError, "RsyncFetch object already closed");

	rf->closed = true;
	rf_clear(rf);

	Py_RETURN_NONE;
}

static PyObject *RsyncFetch_close(PyObject *self) {
	RsyncFetch_t *rf = RsyncFetch_Check(self, true);
	if(!rf)
		return NULL;

	PyThread_type_lock lock = rf->lock;
	PyLockStatus have_lock;
	Py_BEGIN_ALLOW_THREADS
	have_lock = PyThread_acquire_lock(lock, WAIT_LOCK);
	Py_END_ALLOW_THREADS
	if(have_lock != PY_LOCK_ACQUIRED)
		return PyErr_Format(PyExc_RuntimeError, "unable to acquire lock");

	PyObject *ret = RsyncFetch_close_locked(rf);

	PyThread_release_lock(lock);

	return ret;
}

static PyObject *RsyncFetch_exit_locked(RsyncFetch_t *rf, PyObject *args) {
	rf->closed = true;
	rf_clear(rf);

	Py_RETURN_NONE;
}

static PyObject *RsyncFetch_exit(PyObject *self, PyObject *args) {
	RsyncFetch_t *rf = RsyncFetch_Check(self, true);
	if(!rf)
		return NULL;

	PyThread_type_lock lock = rf->lock;
	PyLockStatus have_lock;
	Py_BEGIN_ALLOW_THREADS
	have_lock = PyThread_acquire_lock(lock, WAIT_LOCK);
	Py_END_ALLOW_THREADS
	if(have_lock != PY_LOCK_ACQUIRED)
		return PyErr_Format(PyExc_RuntimeError, "unable to acquire lock");

	PyObject *ret = RsyncFetch_exit_locked(rf, args);

	PyThread_release_lock(lock);

	return ret;
}

static int RsyncFetch_init_locked(RsyncFetch_t *rf, PyObject *args, PyObject *kwargs) {
	if(rf->closed) {
		PyErr_Format(PyExc_RuntimeError, "RsyncFetch object already closed");
		return -1;
	}

	PyObject *entry_callback = NULL, *error_callback = NULL;
	PyObject *command = NULL, *filters = NULL;
	Py_ssize_t chunk_size = rf->chunk_size;
	static char *keywords[] = { "command", "entry_callback", "error_callback", "filters", "chunk_size", NULL };
	if(!PyArg_ParseTupleAndKeywords(args, kwargs, "|$OOOOn:run", keywords,
			&command, &entry_callback, &error_callback, &filters, &chunk_size))
		return -1;

	if(!command) {
		PyErr_Format(PyExc_TypeError, "missing command parameter");
		return -1;
	}

	free(rf->command);
	rf->command = NULL;
	if(!rf_status_to_exception(rf, rf_iterate(rf, command, &rf->command, NULL)))
		return -1;

	if(!entry_callback) {
		PyErr_Format(PyExc_TypeError, "missing entry_callback parameter");
		return -1;
	}

	if(!PyCallable_Check(entry_callback)) {
		PyErr_Format(PyExc_TypeError, "entry_callback parameter is not callable");
		return -1;
	}
	Py_IncRef(entry_callback);
	Py_CLEAR(rf->entry_callback);
	rf->entry_callback = entry_callback;

	if(error_callback && error_callback != Py_None) {
		if(!PyCallable_Check(error_callback)) {
			PyErr_Format(PyExc_TypeError, "error_callback parameter is not callable");
			return -1;
		}
		Py_IncRef(error_callback);
		Py_CLEAR(rf->error_callback);
		rf->error_callback = error_callback;
	}

	free(rf->filters);
	rf->filters = NULL;
	rf->filters_num = 0;
	if(filters && filters != Py_None) {
		if(!rf_status_to_exception(rf, rf_iterate(rf, filters, &rf->filters, &rf->filters_num)))
			return -1;
	}

	if(chunk_size < 1) {
		PyErr_Format(PyExc_ValueError, "chunk_size must be greater than 0");
		return -1;
	}

	rf->chunk_size = chunk_size;

	return RF_STATUS_OK;
}

static int RsyncFetch_init(PyObject *self, PyObject *args, PyObject *kwargs) {
	RsyncFetch_t *rf = RsyncFetch_Check(self, true);
	if(!rf)
		return -1;

	PyThread_type_lock lock = rf->lock;
	PyLockStatus have_lock;
	Py_BEGIN_ALLOW_THREADS
	have_lock = PyThread_acquire_lock(lock, WAIT_LOCK);
	Py_END_ALLOW_THREADS
	if(have_lock != PY_LOCK_ACQUIRED) {
		PyErr_SetString(PyExc_RuntimeError, "unable to acquire lock");
		return -1;
	}

	int ret = RsyncFetch_init_locked(rf, args, kwargs);

	PyThread_release_lock(lock);

	return ret;
}

static PyObject *RsyncFetch_new(PyTypeObject *subtype, PyObject *args, PyObject *kwargs) {
	struct RsyncFetchObject *obj = PyObject_New(struct RsyncFetchObject, subtype);
	if(obj) {
		obj->rf = RsyncFetch_0;

		PyThread_type_lock lock = PyThread_allocate_lock();
		if(lock) {
			obj->rf.lock = lock;
			return &obj->ob_base;
		}

		RsyncFetch_dealloc(&obj->ob_base);
	}
	return NULL;
}

static PyObject *RsyncFetch_run_locked(RsyncFetch_t *rf) {
	if(rf->closed)
		return PyErr_Format(PyExc_RuntimeError, "RsyncFetch object already closed");
	rf->closed = true;

	if(!rf->entry_callback || !rf->command)
		return PyErr_Format(PyExc_RuntimeError, "RsyncFetch object not initialized properly");

	Py_CLEAR(rf->chunk_bytes);
	PyObject *chunk_bytes = PyBytes_FromStringAndSize(NULL, rf->chunk_size);
	if(!chunk_bytes)
		return NULL;
	rf->chunk_bytes = chunk_bytes;
	rf->chunk_buffer = PyBytes_AS_STRING(chunk_bytes);
	
	rf_unblock_threads(rf);
	rf_status_t s = rf_run(rf);
	rf_block_threads(rf);

	if(rf_status_to_exception(rf, s))
		Py_RETURN_NONE;
	else
		return NULL;
}

static PyObject *RsyncFetch_run(PyObject *self) {
	RsyncFetch_t *rf = RsyncFetch_Check(self, true);
	if(!rf)
		return NULL;

	PyThread_type_lock lock = rf->lock;
	PyLockStatus have_lock;
	Py_BEGIN_ALLOW_THREADS
	have_lock = PyThread_acquire_lock(lock, WAIT_LOCK);
	Py_END_ALLOW_THREADS
	if(have_lock != PY_LOCK_ACQUIRED)
		return PyErr_Format(PyExc_RuntimeError, "unable to acquire lock");

	PyObject *ret = RsyncFetch_run_locked(rf);

	PyThread_release_lock(lock);

	return ret;
}

static PyObject *RsyncFetch_self(PyObject *self, PyObject *args) {
	Py_IncRef(self);
	return self;
}

__attribute__((unused))
static PyObject *RsyncFetch_none(PyObject *self, PyObject *args) {
	Py_RETURN_NONE;
}

static PyMethodDef RsyncFetch_methods[] = {
	{"__enter__", (PyCFunction)RsyncFetch_self, METH_NOARGS, "return a context manager for 'with'"},
	{"__exit__", RsyncFetch_exit, METH_VARARGS, "callback for 'with' context manager"},
	{"close", (PyCFunction)RsyncFetch_close, METH_NOARGS, "close this object"},
	{"run", (PyCFunction)RsyncFetch_run, METH_NOARGS, "perform the rsync action"},
	{NULL}
};

static PyTypeObject RsyncFetch_type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_basicsize = sizeof(struct RsyncFetchObject),
	.tp_name = "rsync_fetch.RsyncFetch",
	.tp_new = RsyncFetch_new,
	.tp_init = RsyncFetch_init,
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

	PyObject *dict = ((PyTypeObject *)&RsyncFetch_type)->tp_dict;
	if(!PyDict_Check(dict))
		return NULL;
	PyObject *options = Py_BuildValue("(yyy)", "--server", "--sender", "-lHogDtpre.iLsf");
	if(!options)
		return NULL;
	int r = PyDict_SetItemString(dict, "required_options", options);
	Py_DecRef(options);
	if(r == -1)
		return NULL;

	PyObject *module = PyModule_Create(&rsync_fetch_module);
	if(module) {
		if(PyModule_AddObject(module, "RsyncFetch", &RsyncFetch_type.ob_base.ob_base) != -1)
			return module;
		Py_DecRef(module);
	}
	return NULL;
}
