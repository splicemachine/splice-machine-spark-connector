NAME = jpanko/ssds_msgct
VERSION = 0.0.6
SPLICE_BASE_IMAGE_VERSION = 0.0.4

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
            --build-arg STATIC_BASE_ARTIFACT_URI=${STATIC_BASE_ARTIFACT_URI} \
            --build-arg SHIRO_CORE_VERSION=${SHIRO_CORE_VERSION} \
            --build-arg SHIRO_WEB_VERSION=${SHIRO_WEB_VERSION} \
            --build-arg SPLICE_SHIRO_VERSION=${SPLICE_SHIRO_VERSION} \
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

