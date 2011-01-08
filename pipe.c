#include "pipe.h"

#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#ifndef min
#define min(a, b) ((a) <= (b) ? (a) : (b))
#endif

#ifndef max
#define max(a, b) ((a) >= (b) ? (a) : (b))
#endif

// Runs a memcpy, then returns the end of the range copied.
// Has identical functionality as mempcpy, but is portable.
static inline void* offset_memcpy(void* restrict dest, const void* restrict src, size_t n)
{
    return (char*)memcpy(dest, src, n) + n;
}

/*
 * Pipe implementation overview
 * =================================
 *
 * A pipe is implemented as a circular buffer. There are two special cases for
 * this structure: nowrap and wrap.
 *
 * Nowrap:
 *
 *     buffer          begin               end                 bufend
 *       [               >==================>                    ]
 *
 * In this case, the data storage is contiguous, allowing easy access. This is
 * the simplest case.
 *
 * Wrap:
 *
 *     buffer        end                 begin                 bufend
 *       [============>                    >=====================]
 *
 * In this case, the data storage is split up, wrapping around to the beginning
 * of the buffer when it hits bufend. Hackery must be done in this case to
 * ensure the structure is maintained and data can be easily copied in/out.
 *
 * Invariants:
 *
 * The invariants of a pipe are documented in the check_invariants function,
 * and double-checked frequently in debug builds. This helps restore sanity when
 * making modifications, but may slow down calls. It's best to disable the
 * checks in release builds.
 *
 * Thread-safety:
 *
 * pipe_t has been designed with high threading workloads foremost in my mind.
 * Its initial purpose was to serve as a task queue, with multiple threads
 * feeding data in (from disk, network, etc) and multiple threads reading it
 * and processing it in parallel. This created the need for a fully re-entrant,
 * lightweight, accomodating data structure.
 *
 * No fancy threading tricks are used thus far. It's just a simple mutex
 * guarding the pipe, with a condition variable to signal when we have new
 * elements so the blocking consumers can get them. If you modify the pipe,
 * lock the mutex. Keep it locked for as short as possible.
 *
 * TODO: Can we use a spinlock instead of a mutex? We still need to condition
 *       variable on it.
 */
struct pipe {
    size_t elem_size;  // The size of each element.
    size_t elem_count; // The number of elements currently in the pipe.
    size_t capacity;   // The maximum number of elements the buffer can hold
                       // before a reallocation.
    size_t min_cap;    // The smallest sane capacity before the buffer refuses
                       // to shrink because it would just end up growing again.

    char * buffer,     // The internal buffer, holding the enqueued elements.
         * bufend,     // Just a shortcut pointer to the end of the buffer.
                       // It helps me not constantly type (p->buffer + p->elem_size*p->capacity).
         * begin,      // Always points to the left-most/first-pushed element in the pipe.
         * end;        // Always points to the right-most/last-pushed element in the pipe.

    size_t producer_refcount;      // The number of producers currently in circulation.
    size_t consumer_refcount;      // The number of consumers currently in circulation.

    pthread_mutex_t m;             // The mutex guarding the WHOLE pipe.
    pthread_cond_t  has_new_elems; // Signaled when the pipe has at least one element in it.
};

// Poor man's typedef. For more information, see DEF_NEW_FUNC's typedef.
struct producer { pipe_t pipe; };
struct consumer { pipe_t pipe; };

// Converts a pointer to either a producer or consumer into a suitable pipe_t*.
#define PIPIFY(producer_or_consumer) (&(producer_or_consumer)->pipe)

// The initial minimum capacity of the pipe. This can be overridden dynamically
// with pipe_reserve.
#ifdef DEBUG
#define DEFAULT_MINCAP  2
#else
#define DEFAULT_MINCAP  32
#endif

// Moves bufend to the end of the buffer, assuming buffer, capacity, and
// elem_size are all sane.
static inline void fix_bufend(pipe_t* p)
{
    p->bufend = p->buffer + p->elem_size * p->capacity;
}

// Does the buffer wrap around?
//   true  -> wrap
//   false -> nowrap
static inline bool wraps_around(const pipe_t* p)
{
    return p->begin > p->end;
}

// Is the pointer `p' within [left, right]?
static inline bool in_bounds(const void* left, const void* p, const void* right)
{
    return p >= left && p <= right;
}

// Wraps the begin (and possibly end) pointers of p to the beginning of the
// buffer if they've hit the end.
static inline char* wrap_if_at_end(char* p, char* begin, const char* end)
{
    return p == end ? begin : p;
}

static inline size_t next_pow2(size_t n)
{
    for(size_t i = 1; i != 0; i <<= 1)
        if(n <= i)
            return i;

    // Holy shit it must be a huge number to get here. I'm scared. Let's not double
    // the size in this case.
    return n;
}

// You know all those assumptions we make about our data structure whenever we
// use it? This function checks them, and is called liberally through the
// codebase. It would be best to read this function over, as it also acts as
// documentation. Code AND documentation? What is this witchcraft?
static void check_invariants(const pipe_t* p)
{
    // Give me valid pointers or give me death!
    assert(p);

    // p->buffer may be NULL. When it is, it means the pipe is in the middle of
    // destruction. If that's the case, we can't check much of anything.
    if(p->buffer == NULL)
        return;

    assert(p->bufend);
    assert(p->begin);
    assert(p->end);

    assert(p->elem_size != 0);

    assert(p->capacity >= p->min_cap && "The buffer's capacity should never drop below the minimum.");
    assert(p->elem_count <= p->capacity && "There are more elements in the buffer than its capacity.");
    assert(p->bufend == p->buffer + p->elem_size*p->capacity && "This is axiomatic. Was fix_bufend not called somewhere?");

    assert(in_bounds(p->buffer, p->begin, p->bufend));
    assert(in_bounds(p->buffer, p->end, p->bufend));

    assert(p->begin != p->bufend && "The begin pointer should NEVER point to the end of the buffer."
                    "If it does, it should have been automatically moved to the front.");

    // Ensure the size accurately reflects the begin/end pointers' positions.
    // Kindly refer to the diagram in struct pipe's documentation =)
    if(wraps_around(p))
        assert(p->elem_size*p->elem_count == (p->bufend - p->begin) + (p->end - p->buffer));
    else
        assert(p->elem_size*p->elem_count == p->end - p->begin);
}

// Enforce is just assert, but runs the expression in release build, instead of
// filtering it out like assert would.
#ifdef NDEBUG
#define ENFORCE(expr) (void)(expr)
#else
#define ENFORCE assert
#endif

static inline void lock_pipe(pipe_t* p)
{
    ENFORCE(pthread_mutex_lock(&p->m) == 0);
    check_invariants(p);
}

static inline void unlock_pipe(pipe_t* p)
{
    check_invariants(p);
    ENFORCE(pthread_mutex_unlock(&p->m) == 0);
}

#define WHILE_LOCKED(stuff) do { lock_pipe(p); stuff; unlock_pipe(p); } while(0)

static inline void init_mutex(pthread_mutex_t* m)
{
    pthread_mutexattr_t attr;

    ENFORCE(pthread_mutexattr_init(&attr) == 0);
    ENFORCE(pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_PRIVATE) == 0);

    ENFORCE(pthread_mutex_init(m, &attr) == 0);
}

pipe_t* pipe_new(size_t elem_size)
{
    assert(elem_size != 0);

    assert(sizeof(pipe_t) == sizeof(consumer_t));
    assert(sizeof(consumer_t) == sizeof(producer_t));

    pipe_t* p = malloc(sizeof(pipe_t));

    if(p == NULL)
        return p;

    p->elem_size = elem_size;
    p->elem_count = 0;
    p->capacity =
    p->min_cap  = DEFAULT_MINCAP;

    p->buffer =
    p->begin  =
    p->end    = malloc(p->elem_size * p->capacity);

    fix_bufend(p);

    p->producer_refcount =
    p->consumer_refcount = 1;    // Since we're issuing a pipe_t, it counts as both
                                 // a pusher and a popper since it can issue
                                 // new instances of both. Therefore, the count of
                                 // both starts at 1 - not the intuitive 0.

    init_mutex(&p->m);

    ENFORCE(pthread_cond_init(&p->has_new_elems, NULL) == 0);

    check_invariants(p);

    return p;
}

// Yes, this is a total hack. What of it?
//
// What we do after incrementing the refcount is casting our pipe to the
// appropriate handle. Since the handle is defined with pipe_t as the
// first member (therefore lying at offset 0), we can secretly pass around
// our pipe_t structure without the rest of the world knowing it. This also
// keeps us from needlessly mallocing (and subsequently freeing) handles.
#define DEF_NEW_FUNC(type)                     \
    type##_t* pipe_##type##_new(pipe_t* p)     \
    {                                          \
        WHILE_LOCKED( ++p->type##_refcount; ); \
        return (type##_t*)p;                   \
    }

DEF_NEW_FUNC(producer)
DEF_NEW_FUNC(consumer)

#undef DEF_NEW_FUNC

static inline bool requires_deallocation(const pipe_t* p)
{
    return p->producer_refcount == 0 && p->consumer_refcount == 0;
}

static inline void deallocate(pipe_t* p)
{
    pthread_mutex_unlock(&p->m);

    pthread_mutex_destroy(&p->m);
    pthread_cond_destroy(&p->has_new_elems);

    free(p->buffer);
    free(p);
}

void pipe_free(pipe_t* p)
{
    assert(p->producer_refcount > 0);
    assert(p->consumer_refcount > 0);

    WHILE_LOCKED(
        --p->producer_refcount;
        --p->consumer_refcount;

        if(requires_deallocation(p))
            return deallocate(p);
    );
}

#define DEF_FREE_FUNC(type)                     \
    void pipe_##type##_free(type##_t* handle)   \
    {                                           \
        pipe_t* restrict p = PIPIFY(handle);    \
                                                \
        assert(p->type##_refcount > 0);         \
                                                \
        WHILE_LOCKED(                           \
            --p->type##_refcount;               \
                                                \
            if(requires_deallocation(p))        \
                return deallocate(p);           \
        );                                      \
    }

DEF_FREE_FUNC(producer)
DEF_FREE_FUNC(consumer)

#undef DEF_FREE_FUNC

// Returns the end of the buffer (buf + number_of_bytes_copied).
static inline char* copy_pipe_into_new_buf(const pipe_t* p, char* buf, size_t bufsize)
{
    assert(bufsize >= p->elem_size * p->elem_count && "Trying to copy into a buffer that's too small.");
    check_invariants(p);

    if(wraps_around(p))
    {
        buf = offset_memcpy(buf, p->begin, p->bufend - p->begin);
        buf = offset_memcpy(buf, p->buffer, p->end - p->buffer);
    }
    else
    {
        buf = offset_memcpy(buf, p->begin, p->end - p->begin);
    }

    return buf;
}

static void resize_buffer(pipe_t* p, size_t new_size)
{
    check_invariants(p);

    // I refuse to resize to a size smaller than what would keep all our
    // elements in the buffer or one that is smaller than the minimum capacity.
    if(new_size <= p->elem_count || new_size < p->min_cap)
        return;

    size_t new_size_in_bytes = new_size*p->elem_size;

    char* new_buf = malloc(new_size_in_bytes);
    p->end = copy_pipe_into_new_buf(p, new_buf, new_size_in_bytes);

    free(p->buffer);

    p->buffer = p->begin = new_buf;
    p->capacity = new_size;

    fix_bufend(p);

    check_invariants(p);
}

static inline void push_without_locking(pipe_t* restrict p, const void* restrict elems, size_t count)
{
    assert(elems && "Trying to push a NULL pointer into the pipe. That just won't do.");
    check_invariants(p);

    if(p->elem_count + count > p->capacity)
        resize_buffer(p, next_pow2(p->elem_count + count));

    // Since we've just grown the buffer (if necessary), we now KNOW we have
    // enough room for the push. So do it

    size_t bytes_to_copy = count*p->elem_size;

    // We cache the end locally to avoid wasteful dereferences of p.
    char* newend = p->end;

    // If we currently have a nowrap buffer, we may have to wrap the new
    // elements. Copy as many as we can at the end, then start copying into the
    // beginning. This basically reduces the problem to only deal with wrapped
    // buffers, which can be dealt with using a single offset_memcpy.
    if(!wraps_around(p))
    {
        size_t at_end = min(bytes_to_copy, p->bufend - p->end);

        newend = wrap_if_at_end(
                     offset_memcpy(newend, elems, at_end),
                     p->buffer, p->bufend);

        elems = (const char*)elems + at_end;
        bytes_to_copy -= at_end;
    }

    // Now copy any remaining data...
    newend = wrap_if_at_end(
                 offset_memcpy(newend, elems, bytes_to_copy),
                 p->buffer, p->bufend
             );

    // ...and update the end pointer and count!
    p->end         = newend;
    p->elem_count += count;

    check_invariants(p);
}

void pipe_push(producer_t* restrict prod, const void* restrict elems, size_t count)
{
    pipe_t *restrict p = PIPIFY(prod);

    if(p == NULL)
        return;

    WHILE_LOCKED(
        push_without_locking(p, elems, count);
    );

    pthread_cond_broadcast(&p->has_new_elems);
}

// wow, I didn't even intend for the name to work like that...
static inline size_t pop_without_locking(pipe_t* restrict p, void* restrict target, size_t count)
{
    assert(target && "Why are we trying to pop elements out of a pipe and into a NULL buffer?");
    check_invariants(p);

    const size_t elems_to_copy   = min(count, p->elem_count);
          size_t bytes_remaining = elems_to_copy * p->elem_size;

    assert(bytes_remaining <= p->elem_count*p->elem_size);

    // We're about to pop the elements. Fix the count now.
    p->elem_count -= elems_to_copy;

//  Copy [begin, min(bufend, begin + bytes_to_copy)) into target.
    {
        // Copy either as many bytes as requested, or the available bytes in
        // the RHS of a wrapped buffer - whichever is smaller.
        size_t first_bytes_to_copy = min(bytes_remaining, p->bufend - p->begin);

        target = offset_memcpy(target, p->begin, first_bytes_to_copy);

        bytes_remaining -= first_bytes_to_copy;
        p->begin         = wrap_if_at_end(
                               p->begin + first_bytes_to_copy,
                               p->buffer, p->bufend);
    }

    // If we're dealing with a wrap buffer, copy the remaining bytes
    // [buffer, buffer + bytes_to_copy) into target.
    if(bytes_remaining > 0)
    {
        memcpy(target, p->buffer, bytes_remaining);
        p->begin = wrap_if_at_end(p->begin + bytes_remaining, p->buffer, p->bufend);
    }    

    check_invariants(p);

    // To conserve space like the good computizens we are, we'll shrink
    // our buffer if our memory usage efficiency drops below 25%. However,
    // since shrinking/growing the buffer is the most expensive part of a push
    // or pop, we only shrink it to bring us up to a 50% efficiency. A common
    // pipe usage pattern is sudden bursts of pushes and pops. This ensures it
    // doesn't get too time-inefficient.
    if(p->elem_count <= (p->capacity / 4))
        resize_buffer(p, p->capacity / 2);

    return elems_to_copy;
}

size_t pipe_pop(consumer_t* restrict c, void* restrict target, size_t count)
{
    pipe_t* restrict p = PIPIFY(c);

    if(p == NULL)
        return 0;

    size_t ret;

    WHILE_LOCKED(
        // While we need more elements and there exists at least one producer...
        while(p->elem_count < count && p->producer_refcount > 0)
            pthread_cond_wait(&p->has_new_elems, &p->m);

        ret = p->elem_count > 0
              ? pop_without_locking(p, target, count)
              : 0;
    );

    return ret;
}

void pipe_reserve(pipe_t* p, size_t count)
{
    if(p == NULL)
        return;

    if(count == 0)
        count = DEFAULT_MINCAP;

    WHILE_LOCKED(
        if(count > p->elem_count)
        {
            p->min_cap = count;
            resize_buffer(p, count);
        }
    );
}