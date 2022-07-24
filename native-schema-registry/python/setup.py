import os
from distutils.core import setup
from distutils.extension import Extension

# Libraries generated by CMake compilation
libraries = ["nativeschemaregistry", "native_schema_registry_c", "native_schema_registry_c_data_types", "aws_common_memalloc"]
lib_name = "PyGsrSerDe"

library_dirs = [lib_name]
swig_module_name = lib_name + "._GsrSerDePyGen"
swig_sources = [os.path.join(lib_name, "glue_schema_registry_serdePYTHON_wrap.c")]
include_dir = [os.path.join(os.getcwd(), "..", "c", "include")]
compiler_flags = ["-ggdb3"]

# Note: Ideally we can compile this file using CMake but
# Python extension needs a single source file to generate.
extension = Extension(
    name=swig_module_name,
    sources=swig_sources,
    include_dirs=include_dir,
    library_dirs=library_dirs,
    libraries=libraries,
    extra_compile_args=compiler_flags,
    language='c')

setup(
    name=lib_name,
    version='0.1',
    py_modules=[lib_name],
    ext_modules=[extension],
    packages=[lib_name],
    install_requires=[
        'fastavro==1.5.2'
    ]
)