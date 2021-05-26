NAME = jpanko/kafka_spark_dev
VERSION = spark3.0.1-5
SPLICE_BASE_IMAGE_VERSION = 0.0.3

CDH_VERSION=cdh6.3.2-1605554
SPARK_VERSION = 3.0.1
HADOOP_VERSION = 3.2

SPLICE_BUILD=3.1.0.2002
SPLICE_BUILD_SUFFIX=p0.100
SPLICE_CDH_VERSION=dbaas3.0
#SPLICEMACHINE_URI=https://splice-releases.s3.amazonaws.com/${SPLICE_BUILD}/cluster/parcel/${SPLICE_CDH_VERSION}/SPLICEMACHINE-${SPLICE_BUILD}.${SPLICE_CDH_VERSION}.${SPLICE_BUILD_SUFFIX}-el7.parcel
SPLICEMACHINE_URI=https://jpanko-splice.s3.amazonaws.com/SPLICEMACHINE-${SPLICE_BUILD}.${SPLICE_CDH_VERSION}.${SPLICE_BUILD_SUFFIX}-el7.parcel

SPLICE_FOLDER_NAME=SPLICEMACHINE-${SPLICE_BUILD}.${SPLICE_CDH_VERSION}.${SPLICE_BUILD_SUFFIX}
STATIC_BASE_ARTIFACT_URI = https://s3.amazonaws.com/splicemachine/artifacts

SHIRO_CORE_VERSION=shiro-core-1.2.3
SHIRO_WEB_VERSION=shiro-web-1.2.3
SPLICE_SHIRO_VERSION=splice-shiro-2.7.0.1915



.PHONY: all build clean push realclean

all: build

run:
	docker run $(NAME):$(VERSION)

build:
	docker build --rm   \
	    --build-arg splice_base_image_version=${SPLICE_BASE_IMAGE_VERSION} \
	    --build-arg SPARK_VERSION=${SPARK_VERSION} \
	    --build-arg CDH_VERSION=${CDH_VERSION}  \
	    --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
        --build-arg STATIC_BASE_ARTIFACT_URI=${STATIC_BASE_ARTIFACT_URI} \
        --build-arg SPLICEMACHINE_URI=${SPLICEMACHINE_URI} \
        --build-arg SPLICE_FOLDER_NAME=${SPLICE_FOLDER_NAME} \
        --build-arg SHIRO_CORE_VERSION=${SHIRO_CORE_VERSION} \
        --build-arg SHIRO_WEB_VERSION=${SHIRO_WEB_VERSION} \
        --build-arg SPLICE_SHIRO_VERSION=${SPLICE_SHIRO_VERSION} \
	    --build-arg SPLICE_BUILD=${SPLICE_BUILD} \
        -t $(NAME):$(VERSION) .
	docker tag $(NAME):$(VERSION) $(NAME):latest

push:
	docker push $(NAME):latest
	docker push $(NAME):$(VERSION)

clean:
	-docker rmi $(NAME):latest
	-docker rmi $(NAME):$(VERSION)

realclean:
	-docker kill $(shell docker ps -q)
	-docker rm $(shell docker ps -a -q)
	-docker rmi $(shell docker images -q)
	-docker system prune -f -a

