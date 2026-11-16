/* Optional entitlement preflight. No subscriptions, child processes or IPC.
 * Creating/deleting this client is not a process-tree close receipt.
 */
#include <EndpointSecurity/EndpointSecurity.h>
#include <stdio.h>
#include <string.h>

static const char *client_status(es_new_client_result_t result) {
    switch (result) {
    case ES_NEW_CLIENT_RESULT_SUCCESS: return "available";
    case ES_NEW_CLIENT_RESULT_ERR_NOT_ENTITLED: return "missing_entitlement";
    case ES_NEW_CLIENT_RESULT_ERR_NOT_PRIVILEGED: return "requires_root";
    case ES_NEW_CLIENT_RESULT_ERR_NOT_PERMITTED: return "requires_tcc_approval";
    case ES_NEW_CLIENT_RESULT_ERR_TOO_MANY_CLIENTS: return "client_limit_reached";
    case ES_NEW_CLIENT_RESULT_ERR_INVALID_ARGUMENT: return "invalid_argument";
    case ES_NEW_CLIENT_RESULT_ERR_INTERNAL: return "internal_error";
    default: return "unknown_error";
    }
}
int main(int argc, const char *argv[]) {
    if (argc != 2 || strcmp(argv[1], "--check") != 0) {
        fputs("usage: zero-es-preflight --check\n", stderr);
        return 64;
    }
    es_client_t *client = NULL;
    es_new_client_result_t result = es_new_client(&client,
        ^(es_client_t *source, const es_message_t *message) {
            /* No es_subscribe call: no system events are requested or logged. */
            (void)source;
            (void)message;
        });
    if (result != ES_NEW_CLIENT_RESULT_SUCCESS) {
        printf("{\"endpoint_security_client\":\"%s\","
               "\"process_tree_provider\":\"unqualified\"}\n", client_status(result));
        return 2;
    }
    /* Same thread, exactly the native client returned by es_new_client. */
    es_return_t closed = es_delete_client(client);
    printf("{\"endpoint_security_client\":\"available\","
           "\"client_disposal\":\"%s\","
           "\"process_tree_provider\":\"unqualified\"}\n",
           closed == ES_RETURN_SUCCESS ? "confirmed" : "unconfirmed");
    return closed == ES_RETURN_SUCCESS ? 0 : 3;
}
