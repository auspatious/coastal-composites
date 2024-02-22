IMAGE=local/composites

build:
	docker build --tag ${IMAGE} .

run:
	docker run -it --rm ${IMAGE}\
		python src/run_task.py \
		--region-code "9,19" \
		--year 2023 \
		--version "0.0.0" \
		--resolution 100 \
		--output-bucket files.auspatious.com \
		--overwrite
