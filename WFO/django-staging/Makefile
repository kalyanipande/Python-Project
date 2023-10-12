DOCKER_REG=ava-docker-local.esisoj70.emea.nsn-net.net
DOCKER_CUSTOMER=ava-docker-local.esisoj70.emea.nsn-net.net/ava/customers/


TAG=dev-1
LOCAL=django-webapp:${TAG}
REMOTE=${DOCKER_CUSTOMER}${LOCAL}

build: export DECRYPTOR_IMAGE_TAG="4"
build: Dockerfile
	docker login --username vignshan --password AP2Yje3FgQPetvwS9B9Xs3obFDL ${DOCKER_REG}
	sed -i "s/DECRYPTOR_IMAGE_TAG/${DECRYPTOR_IMAGE_TAG}/g" Dockerfile
	docker build -t ${LOCAL} -f Dockerfile .
	docker tag ${LOCAL} ${REMOTE}

build-push: build
	docker push ${REMOTE}
