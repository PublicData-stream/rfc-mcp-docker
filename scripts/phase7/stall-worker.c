/* Disposable substitute for testing the production supervisor under load. */
#include <poll.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    for (int index = 1; index + 1 < argc; ++index) {
        if (strcmp(argv[index], "-e") != 0)
            continue;
        if (strcmp(argv[index + 1], "rfc-worker-check") == 0) {
            puts("{\"type\":\"match\",\"data\":{\"absolute_offset\":0,\"line_number\":1,"
                 "\"lines\":{\"text\":\"rfc-worker-check\\n\"},"
                 "\"submatches\":[{\"match\":{\"text\":\"rfc-worker-check\"},"
                 "\"start\":0,\"end\":16}]}}");
            puts("{\"type\":\"summary\",\"data\":{}}");
            return 0;
        }
        if (strcmp(argv[index + 1], "stall") == 0) {
            poll(NULL, 0, -1);
            return 2;
        }
    }
    return 2;
}
