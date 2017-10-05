# To build a new docker image, run the following from the root source dir:
# $ docker build . -f docker/Dockerfile -t $IMAGE_NAME

FROM google/cloud-sdk

# TODO(arostami): Figure out why 'setup.py install' does't work.
RUN pip install apache_beam[gcp] pyvcf

RUN mkdir -p /opt/variant_processing/bin && mkdir -p /opt/variant_processing/src
ADD / /opt/variant_processing/src/
RUN echo 'python /opt/variant_processing/src/vcf_to_bq.py "$@"' > \
      /opt/variant_processing/bin/vcf_to_bq && \
    chmod +x /opt/variant_processing/bin/vcf_to_bq
