#ifndef ZERO_FFI_H
#define ZERO_FFI_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Start a Zero proxy instance from a configuration file.
 *
 * Returns an opaque handle on success, or NULL on failure.
 * Call zero_last_error() for details on failure.
 */
void *zero_start(const char *config_path);

/**
 * Shut down a Zero proxy instance and free resources.
 *
 * After this call the handle is no longer valid.
 */
void zero_shutdown(void *handle);

/**
 * Execute a read-only query against the running instance.
 *
 * @param handle       Proxy handle returned by zero_start().
 * @param request_json JSON-encoded QueryRequest object.
 * @return             JSON-encoded response string (caller must free with zero_free_string),
 *                     or NULL on error (call zero_last_error()).
 */
char *zero_query(void *handle, const char *request_json);

/**
 * Execute a command against the running instance.
 *
 * @param handle       Proxy handle returned by zero_start().
 * @param command_json JSON-encoded CommandRequest object.
 * @return             JSON-encoded response string (caller must free with zero_free_string),
 *                     or NULL on error (call zero_last_error()).
 */
char *zero_execute(void *handle, const char *command_json);

/**
 * Free a string previously returned by zero_query() or zero_execute().
 *
 * Passing NULL is safe (no-op).
 */
void zero_free_string(char *s);

/**
 * Return the last error message for the current thread.
 *
 * The returned pointer is valid until the next Zero FFI call on this thread.
 * Returns NULL if no error has occurred.
 */
const char *zero_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* ZERO_FFI_H */
