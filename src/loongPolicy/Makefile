#
# Note: This makefile depends on 2 environment variables to funtion correctly:
# a) BASE_DIR
# b) BUILD_DIR
# C) TARGET_LIB_DIR
# All these are passed by build.xml.
#

ifeq ("x$(JAVA_HOME)","x")
  export JAVA_HOME=/usr/local/jdk1.6.0_41
  source ~/.bash_profile
  echo ${JAVA_HOME}
endif

CC = gcc
CFLAGS = -shared -fpic
JAVAH = javah
JFLAGS = -jni

LOONGPOLICY_DIR = ${BASE_DIR}/loongPolicy
CLASS_DIR = ${BUILD_DIR}/metastore/classes
CLASS_PATH = org/apache/hadoop/hive/metastore

LIB_NAME = LoongStorePolicy.c
SO_LINK_NAME = libLoongStorePolicy.so
CLASS_NAME = org.apache.hadoop.hive.metastore.LoongStorePolicy
JAVAH_NAME = org_apache_hadoop_hive_metastore_LoongStorePolicy.h

INCLUDE_PATHS = -I${JAVA_HOME}/include \
		-I${JAVA_HOME}/include/linux \
		-I${CLASS_DIR}

all:: ${LIB_NAME}
	mkdir -p ${CLASS_PATH}
	cp ${CLASS_DIR}/${CLASS_PATH}/LoongStorePolicy.class ${CLASS_PATH}
	${JAVAH} ${JFLAGS} ${CLASS_NAME}
	${CC} ${CFLAGS} ${INCLUDE_PATHS} $< -o ${SO_LINK_NAME}
	cp ${SO_LINK_NAME} ${TARGET_LIB_DIR}

clean:
	rm  ${LOONGPOLICY_DIR}/${SO_LINK_NAME}
	rm  ${LOONGPOLICY_DIR}/org_apache_hadoop_hive_metastore_LoongStorePolicy*
	rm -rf ${LOONGPOLICY_DIR}/org/
