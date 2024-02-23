IMAGE=local/composites

build:
	docker build --tag ${IMAGE} .

build-clean:
	docker build --no-cache --tag ${IMAGE} .

run:
	docker run -it --rm \
		-p 8787:8787 \
		-v /home/ubuntu/tide_models:/tide_models \
		${IMAGE}\
		python src/run_task.py \
		--tile-id "13,45" \
		--year 2022 \
		--version "0.0.0" \
		--low-or-high low \
		--extra-months 6 \
		--tide-data-location /tide_models \
		--output-resolution 10 \
		--output-bucket files.auspatious.com \
		--overwrite
