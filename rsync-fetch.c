#define _LARGEFILE64_SOURCE 1

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>

#define PY_SSIZE_T_CLEAN

#include "Python.h"
#include "pythread.h"

#ifdef WITH_THREAD
#define DECLARE_THREAD_SAVE PyThreadState *_save;
#else
#define DECLARE_THREAD_SAVE
#endif

typedef struct RsyncFetch {
	PyObject_HEAD
	uint64_t magic;
	int infd;
	int outfd;
	int pid;
} RsyncFetch_t;
static const RsyncFetch_t RsyncFetch_0 = {.infd = -1, .outfd = -1};

static struct PyModuleDef rsync_fetch_module;
static PyTypeObject RsyncFetch_type;

#define RSYNCFETCH_MAGIC UINT64_C(0x6FB32179D3F495D0)

static inline RsyncFetch_t *RsyncFetch_Check(PyObject *v) {
	return v && PyObject_TypeCheck(v, &RsyncFetch_type) && ((RsyncFetch_t *)v)->magic == RSYNCFETCH_MAGIC
		? (RsyncFetch_t *)v
		: NULL;
}

static PyObject *RsyncFetch_new(PyTypeObject *subtype, PyObject *args, PyObject *kwargs) {
	RsyncFetch_t *rf;
	rf = PyObject_New(RsyncFetch_t, subtype);
	if(!rf)
		return NULL;

	rf->magic = RSYNCFETCH_MAGIC;
	return &rf->ob_base;
}

static int RsyncFetch_dealloc(PyObject *self) {
	RsyncFetch_t *rf = RsyncFetch_Check(self);
	if(rf) {
		rf->magic = 0;
	}

	freefunc tp_free = Py_TYPE(self)->tp_free ?: PyObject_Free;
	tp_free(self);

	return 0;
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

