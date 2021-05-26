#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version


ARG CDH_VERSION

# Spark dependencies
ARG SPARK_VERSION
ARG HADOOP_VERSION

ARG SPLICE_BUILD
ARG STATIC_BASE_ARTIFACT_URI
ARG SPLICEMACHINE_URI
ARG SPLICE_FOLDER_NAME
ARG SPLICE_PARCEL_NAME=${SPLICE_FOLDER_NAME}-el7.parcel


ARG SHIRO_CORE_VERSION
ARG SHIRO_WEB_VERSION
ARG SPLICE_SHIRO_VERSION


ARG SHIRO_CORE_URI=$STATIC_BASE_ARTIFACT_URI/${SHIRO_CORE_VERSION}.jar
ARG SHIRO_WEB_URI=$STATIC_BASE_ARTIFACT_URI/${SHIRO_WEB_VERSION}.jar
ARG SPLICE_SHIRO_URI=$STATIC_BASE_ARTIFACT_URI/${SPLICE_SHIRO_VERSION}.jar

ARG SPARK_NAME=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ARG SPARK_URI=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz


ENV SPARK_HOME=/opt/spark
ENV SPARK_LOG_DIR=/var/log/spark
ENV SPARK_PID_DIR=/var/run/spark

ENV SPLICEMACHINE_HOME=/opt/splicemachine


RUN echo $SPARK_URI
#Install spark
 RUN cd /tmp && \
    wget -q $SPARK_URI && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt --owner root --group root --no-same-owner && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
 RUN cd /opt && ln -s spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

RUN \
  mkdir -p $SPARK_LOG_DIR && \
  mkdir -p $SPARK_PID_DIR && \
  mkdir -p /target && \
  chmod +w /target

RUN ls /opt
RUN ls /opt/spark
RUN ls $SPARK_HOME
# Copy Shiro for authentication
RUN  cd $SPARK_HOME/jars && \
    wget -q  $SHIRO_CORE_URI      && \
   wget -q   $SHIRO_WEB_URI   && \
   wget -q   $SPLICE_SHIRO_URI

#Download Splicemachine
RUN  cd /tmp && \
  wget -q   ${SPLICEMACHINE_URI}  && \
  tar xzf ${SPLICE_PARCEL_NAME} && \
  mv /tmp/${SPLICE_FOLDER_NAME} $SPLICEMACHINE_HOME && \
  cp $SPLICEMACHINE_HOME/lib/*  $SPARK_HOME/jars/  &&  \
  rm ${SPLICE_PARCEL_NAME}

COPY ./jars/*.jar $SPARK_HOME/jars/
COPY ./scripts/* /
COPY ./conf/ $SPARK_HOME/conf/

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

RUN ls ${SPARK_HOME}/bin

CMD ["/run-spark-application.sh"]
