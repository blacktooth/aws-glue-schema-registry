#include "../include/glue_schema_registry_schema.h"
#include <stdlib.h>
#include "cmocka.h"

#define TEST_SCHEMA_NAME "Employee.proto"
#define TEST_DATA_FORMAT "PROTOBUF"
#define TEST_SCHEMA_DEF "message Employee { string name = 1; int32 rank = 2;}"

void assert_gsr_schema(glue_schema_registry_schema expected, glue_schema_registry_schema actual) {
    assert_string_equal(expected.schema_name, actual.schema_name);
    assert_string_equal(expected.schema_def, actual.schema_def);
    assert_string_equal(expected.data_format, actual.data_format);
}

void schema_cleanup(glue_schema_registry_schema * obj) {
    delete_glue_schema_registry_schema(obj);
}

static void glue_schema_registry_schema_deletes_the_instance(void **state) {
    glue_schema_registry_schema * gsr_schema =
            new_glue_schema_registry_schema(TEST_SCHEMA_NAME, TEST_SCHEMA_DEF, TEST_DATA_FORMAT);

    delete_glue_schema_registry_schema(gsr_schema);
}

static void glue_schema_registry_schema_creates_new_glue_schema_registry_schema(void **state) {
    glue_schema_registry_schema * gsr_schema =
            new_glue_schema_registry_schema(TEST_SCHEMA_NAME, TEST_SCHEMA_DEF, TEST_DATA_FORMAT);

    glue_schema_registry_schema expected;
    expected.data_format = TEST_DATA_FORMAT;
    expected.schema_def = TEST_SCHEMA_DEF;
    expected.schema_name = TEST_SCHEMA_NAME;

    assert_gsr_schema(expected, *gsr_schema);
    schema_cleanup(gsr_schema);
}

static void glue_schema_registry_schema_get_attribute_tests(void **state) {
    glue_schema_registry_schema * gsr_schema =
            new_glue_schema_registry_schema(TEST_SCHEMA_NAME, TEST_SCHEMA_DEF, TEST_DATA_FORMAT);

    assert_string_equal(TEST_DATA_FORMAT, glue_schema_registry_schema_get_data_format(gsr_schema));
    assert_string_equal(TEST_SCHEMA_NAME, glue_schema_registry_schema_get_schema_name(gsr_schema));
    assert_string_equal(TEST_SCHEMA_DEF, glue_schema_registry_schema_get_schema_def(gsr_schema));

    schema_cleanup(gsr_schema);
}

static void glue_schema_registry_schema_when_NULLs_are_passed_does_not_initialize(void **state) {
    glue_schema_registry_schema * gsr_schema;

    gsr_schema = new_glue_schema_registry_schema(NULL, TEST_SCHEMA_DEF, TEST_DATA_FORMAT);
    assert_null(gsr_schema);

    gsr_schema = new_glue_schema_registry_schema(TEST_SCHEMA_NAME, NULL, TEST_DATA_FORMAT);
    assert_null(gsr_schema);

    gsr_schema = new_glue_schema_registry_schema(TEST_SCHEMA_NAME, TEST_SCHEMA_DEF, NULL);
    assert_null(gsr_schema);

    schema_cleanup(gsr_schema);
}

static void glue_schema_registry_schema_when_initialized_with_EmptyString_does_not_fail(void **state) {
    glue_schema_registry_schema * gsr_schema =
            new_glue_schema_registry_schema("", "", "");

    assert_string_equal("", glue_schema_registry_schema_get_data_format(gsr_schema));
    assert_string_equal("", glue_schema_registry_schema_get_schema_name(gsr_schema));
    assert_string_equal("", glue_schema_registry_schema_get_schema_def(gsr_schema));

    schema_cleanup(gsr_schema);
}

int main() {
    const struct CMUnitTest tests[] = {
            cmocka_unit_test(glue_schema_registry_schema_deletes_the_instance),
            cmocka_unit_test(glue_schema_registry_schema_when_NULLs_are_passed_does_not_initialize),
            cmocka_unit_test(glue_schema_registry_schema_when_initialized_with_EmptyString_does_not_fail),
            cmocka_unit_test(glue_schema_registry_schema_get_attribute_tests),
            cmocka_unit_test(glue_schema_registry_schema_creates_new_glue_schema_registry_schema)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}