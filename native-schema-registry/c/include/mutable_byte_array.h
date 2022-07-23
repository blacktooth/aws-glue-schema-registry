#ifndef MUTABLE_BYTE_ARRAY_H
#define MUTABLE_BYTE_ARRAY_H
#include "glue_schema_registry_error.h"
#include <stdlib.h>

/* Integer.MAX_VALUE in Java. This gives ~2.1Gb limit on a record. */
#define MAX_BYTES_LIMIT 2147483647
/**
 * A mutable byte array that allows write / updating bytes in a fixed array of
 * size `max_len`.
 */
typedef struct mutable_byte_array {
    struct aws_byte_buf *byte_buf;
} mutable_byte_array;

/**
 * Initializes a mutable byte array of size `len`
 * Caller can optionally provide pointer holder to glue_schema_registry_error to
 * read error messages.
 */
mutable_byte_array *new_mutable_byte_array(size_t len, glue_schema_registry_error **p_err);

/**
 * Free the data and the pointer to the mutable byte array.
 */
void delete_mutable_byte_array(mutable_byte_array *array);

/**
 * Get the reference to the array contents.
 */
unsigned char *mutable_byte_array_get_data(const mutable_byte_array *array);

/**
 * Writes a single byte at given index in the byte array.
 */
void mutable_byte_array_write(mutable_byte_array *array, unsigned char byte, glue_schema_registry_error **p_err);

/**
 * Return the len of the byte-array
 */
size_t mutable_byte_array_get_max_len(const mutable_byte_array *array);

/**
 * Copies the underlying buffer to the destination.
 */
void mutable_byte_array_copy_data(const mutable_byte_array *array, unsigned char *dst);

#endif /* MUTABLE_BYTE_ARRAY_H */
