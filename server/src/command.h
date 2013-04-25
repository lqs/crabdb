#include <msgpack.h>
#include "server.h"

int execute_command(struct request *request, uint32_t command, msgpack_object o, msgpack_packer* response);
