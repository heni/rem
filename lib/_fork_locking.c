#if defined(_win_)
#error Windows is not supported
#endif

#include <pthread.h>
#include <Python.h>

#if defined(__clang__)
  #define POD_THREAD(T) __thread T
  #define POD_STATIC_THREAD(T) static __thread T
#elif defined __GNUC__ && (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ > 2)) && !(defined __FreeBSD__ && __FreeBSD__ < 5) && !defined(_cygwin_) && !defined(_arm_) && !defined(__IOS_SIMULATOR__)
  #define POD_THREAD(T) __thread T
  #define POD_STATIC_THREAD(T) static __thread T
#elif defined(_arm_)
  #define POD_THREAD(T) __declspec(thread) T
  #define POD_STATIC_THREAD(T) __declspec(thread) static T
#else
  #error No tls for your architecture
#endif

typedef unsigned char bool;
#define false 0
#define true 1

#define DEBUG_FORK_LOCKS 0

#if DEBUG_FORK_LOCKS
extern void fld_write_debug(const char* str, size_t len);
extern void fld_set_debug_fd(int fd);
extern void fld_add_acquire_time_point(size_t locked_thread_count);
#endif

#if DEBUG_FORK_LOCKS
    #define WRITE_DEBUG(s) fld_write_debug("+ "s"\n", sizeof("+ "s"\n"))
#else
    #define WRITE_DEBUG(s)
#endif

POD_STATIC_THREAD(size_t) thread_lock_count = 0;

static volatile size_t forking = false;
static volatile size_t locked_thread_count = 0;

static pthread_mutex_t lock;

static pthread_cond_t all_locks_released;
static pthread_cond_t fork_finished;


static inline void
_wait_for(pthread_mutex_t* lock, pthread_cond_t* cond, volatile size_t* value, size_t expected) {
    if (*value == expected) {
        return;
    }

    pthread_mutex_lock(lock);
    while (*value != expected) {
        Py_BEGIN_ALLOW_THREADS

        pthread_cond_wait(cond, lock);
        pthread_mutex_unlock(lock);

        Py_END_ALLOW_THREADS
        pthread_mutex_lock(lock);
    }
    pthread_mutex_unlock(lock);
}

static inline bool
_acquire_fork(void)
{
    if (forking) {
        return false;
    }

    _wait_for(&lock, &all_locks_released, &locked_thread_count, 0);
    forking = true;

    return true;
}

static inline bool
_release_fork(void)
{
    if (!forking) {
        return false;
    }

    pthread_mutex_lock(&lock);
    forking = false;
    pthread_mutex_unlock(&lock);

    pthread_cond_broadcast(&fork_finished);

    return true;
}

static inline void
_acquire_lock(void)
{
    #if DEBUG_FORK_LOCKS
    fld_add_acquire_time_point(locked_thread_count);
    #endif

    _wait_for(&lock, &fork_finished, &forking, 0);

    if (++thread_lock_count == 1) {
        ++locked_thread_count;
    }
}

static inline bool
_release_lock(void)
{
    if (!thread_lock_count) {
        return false;
    }

    if (!--thread_lock_count) {
        if (locked_thread_count == 1) {
            pthread_mutex_lock(&lock);
            locked_thread_count = 0;
            pthread_mutex_unlock(&lock);

            pthread_cond_signal(&all_locks_released);
        } else {
            --locked_thread_count;
        }
    }

    return true;
}

static PyObject *
acquire_fork(PyObject *self)
{
    (void)self;

    WRITE_DEBUG("acquire_fork before");
    if (!_acquire_fork()) {
        PyErr_SetString(PyExc_RuntimeError, "Fork is already locked");
        return NULL;
    }
    WRITE_DEBUG("acquire_fork acquired");

    Py_RETURN_NONE;
}

static PyObject *
release_fork(PyObject *self)
{
    (void)self;

    WRITE_DEBUG("release_fork");
    if (!_release_fork()) {
        PyErr_SetString(PyExc_RuntimeError, "Fork is not locked");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *
acquire_lock(PyObject *self)
{
    (void)self;
    _acquire_lock();
    Py_RETURN_NONE;
}

static PyObject *
release_lock(PyObject *self)
{
    (void)self;
    if (!_release_lock()) {
        PyErr_SetString(PyExc_RuntimeError, "Thread lock count is zero");
        return NULL;
    }
    Py_RETURN_NONE;
}

#if DEBUG_FORK_LOCKS
static PyObject *
set_debug_fd(PyObject *self, PyObject *args)
{
    (void)self;
    int fd;

    if (!PyArg_ParseTuple(args, "i:set_debug_fd", &fd))
        return NULL;

    fld_set_debug_fd(fd);

    Py_RETURN_NONE;
}
#endif

static PyMethodDef module_methods[] = {
    {"acquire_fork",  (PyCFunction)acquire_fork, METH_NOARGS, NULL},
    {"release_fork",  (PyCFunction)release_fork, METH_NOARGS, NULL},
    {"acquire_lock",  (PyCFunction)acquire_lock, METH_NOARGS, NULL},
    {"release_lock",  (PyCFunction)release_lock, METH_NOARGS, NULL},
#if DEBUG_FORK_LOCKS
    {"set_debug_fd",  (PyCFunction)set_debug_fd, METH_VARARGS, NULL},
#endif
    {NULL,            NULL,                      0,           NULL}
};

#if PY_MAJOR_VERSION >= 3
struct module_state {
    PyObject* error;
};
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
static int module_traverse(PyObject* m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->error);
    return 0;
}
static int module_clear(PyObject* m) {
    Py_CLEAR(GETSTATE(m)->error);
    return 0;
}
static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    "_fork_locking",
    NULL,
    sizeof(struct module_state),
    module_methods,
    NULL,
    module_traverse,
    module_clear,
    NULL
};

PyObject* PyInit__fork_locking(void) {
    PyObject* module = PyModule_Create(&module_def);
    return module;
}
#else
PyMODINIT_FUNC
init_fork_locking(void)
{
    Py_InitModule3("_fork_locking", module_methods, NULL);
}
#endif

