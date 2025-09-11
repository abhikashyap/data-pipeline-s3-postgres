# # Spark 3.5.1 + Java pre-aligned by Apache
# FROM apache/spark:3.5.1

# USER root

# # System tools + Python
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#         python3 python3-pip python3-venv curl ca-certificates && \
#     rm -rf /var/lib/apt/lists/*

# # Python libs (pin pyspark to Spark 3.5.1)
# RUN pip3 install --no-cache-dir \
#       pyspark==3.5.1 \
#       jupyterlab \
#       boto3 \
#       psycopg2-binary \
#       jupyterlab-lsp \
#       python-lsp-server \
#       python-lsp-black \
#       python-lsp-ruff \
#       pylsp-mypy \
#       pylsp-rope

# ENV SPARK_HOME=/opt/spark
# ENV PATH=${SPARK_HOME}/bin:${PATH}
# # NOTE: do NOT override JAVA_HOME — the base image already sets it correctly.

# # Add S3 + Postgres + Redshift jars directly into Spark's classpath
# RUN mkdir -p ${SPARK_HOME}/jars-extra && \
#     curl -fsSL -o ${SPARK_HOME}/jars-extra/hadoop-aws-3.3.4.jar \
#       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
#     curl -fsSL -o ${SPARK_HOME}/jars-extra/aws-java-sdk-bundle-1.12.262.jar \
#       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
#     curl -fsSL -o ${SPARK_HOME}/jars-extra/postgresql-42.6.0.jar \
#       https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
#     curl -fsSL -o ${SPARK_HOME}/jars-extra/RedshiftJDBC42-2.1.0.27.jar \
#       https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/2.1.0.27/redshift-jdbc42-2.1.0.27.jar && \
#     # spark-redshift community connector (fast S3-based loads)
#     curl -fsSL -o ${SPARK_HOME}/jars-extra/spark-redshift_2.12-5.0.3.jar \
#       https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_2.12/5.0.3/spark-redshift_2.12-5.0.3.jar && \
#     curl -fsSL -o ${SPARK_HOME}/jars-extra/clickhouse-jdbc-0.6.2-all.jar \
#       https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.2/clickhouse-jdbc-0.6.2-all.jar && \
#       cp -v ${SPARK_HOME}/jars-extra/*.jar ${SPARK_HOME}/jars/

# # Default Spark config for S3A (adjust region if needed)
# RUN mkdir -p ${SPARK_HOME}/conf && \
#     printf '%s\n' \
#       'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem' \
#       'spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain' \
#       'spark.hadoop.fs.s3a.path.style.access=true' \
#       'spark.hadoop.fs.s3a.endpoint=s3.ap-south-1.amazonaws.com' \
#       > ${SPARK_HOME}/conf/spark-defaults.conf

# # Node + Yarn (for JupyterLab extensions)
# RUN apt-get update && apt-get install -y curl ca-certificates gnupg && \
#     curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
#     apt-get install -y nodejs && \
#     corepack enable && corepack prepare yarn@1.22.22 --activate && \
#     rm -rf /var/lib/apt/lists/*

# # Non-root user + home
# RUN useradd -ms /bin/bash jovyan && mkdir -p /home/jovyan/work && chown -R jovyan:jovyan /home/jovyan
# USER jovyan
# WORKDIR /home/jovyan

# EXPOSE 8888 4040

# # Jupyter token can be provided at runtime via env JUPYTER_TOKEN
# CMD bash -lc 'jupyter lab --ServerApp.ip=0.0.0.0 --ServerApp.open_browser=False --ServerApp.token="${JUPYTER_TOKEN:-letmein}"'
# Spark 3.5.1 + Java pre-aligned by Apache
FROM apache/spark:3.5.1

USER root

# System tools + Python + GPG
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv curl ca-certificates git gnupg && \
    rm -rf /var/lib/apt/lists/*

# Python libs (pin pyspark to Spark 3.5.1)
RUN pip3 install --no-cache-dir \
      pyspark==3.5.1 \
      jupyterlab \
      notebook \
      ipykernel \
      boto3 \
      psycopg2-binary \
      clickhouse-driver \
      jupyterlab-lsp \
      python-lsp-server \
      python-lsp-black \
      python-lsp-ruff \
      pylsp-mypy \
      pylsp-rope

ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}

# Add JDBC jars
RUN mkdir -p ${SPARK_HOME}/jars-extra && \
    curl -fsSL -o ${SPARK_HOME}/jars-extra/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars-extra/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars-extra/postgresql-42.6.0.jar \
      https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars-extra/RedshiftJDBC42-2.1.0.27.jar \
      https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/2.1.0.27/redshift-jdbc42-2.1.0.27.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars-extra/spark-redshift_2.12-5.0.3.jar \
      https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_2.12/5.0.3/spark-redshift_2.12-5.0.3.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars-extra/clickhouse-jdbc-0.6.2-all.jar \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.2/clickhouse-jdbc-0.6.2-all.jar && \
    cp -v ${SPARK_HOME}/jars-extra/*.jar ${SPARK_HOME}/jars/

# Default Spark config for S3A
RUN mkdir -p ${SPARK_HOME}/conf && \
    printf '%s\n' \
      'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem' \
      'spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain' \
      'spark.hadoop.fs.s3a.path.style.access=true' \
      'spark.hadoop.fs.s3a.endpoint=s3.ap-south-1.amazonaws.com' \
      > ${SPARK_HOME}/conf/spark-defaults.conf

# Node + Yarn for JupyterLab extensions
RUN apt-get update && apt-get install -y curl ca-certificates gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    corepack enable && corepack prepare yarn@1.22.22 --activate && \
    rm -rf /var/lib/apt/lists/*

# Non-root user
RUN useradd -ms /bin/bash jovyan && \
    mkdir -p /home/jovyan/work /home/jovyan/.vscode-server && \
    chown -R jovyan:jovyan /home/jovyan

USER jovyan
WORKDIR /home/jovyan

# 🔑 Register kernel in jovyan's home
RUN python3 -m ipykernel install --user --name spark --display-name "Python (Spark)"

EXPOSE 8888 4040

CMD bash -lc 'jupyter lab --ServerApp.ip=0.0.0.0 --ServerApp.open_browser=False --ServerApp.token="${JUPYTER_TOKEN:-letmein}"'
