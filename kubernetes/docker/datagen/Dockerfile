#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM openjdk:8-jdk-alpine
 

ENV APP_HOME=/opt/app
ENV APP_LOG_DIR=/var/log/app
ENV APP_PID_DIR=/var/run/app

ARG STATIC_BASE_ARTIFACT_URI
ARG SHIRO_CORE_VERSION
ARG SHIRO_WEB_VERSION
ARG SPLICE_SHIRO_VERSION

ARG SHIRO_CORE_URI=$STATIC_BASE_ARTIFACT_URI/${SHIRO_CORE_VERSION}.jar
ARG SHIRO_WEB_URI=$STATIC_BASE_ARTIFACT_URI/${SHIRO_WEB_VERSION}.jar
ARG SPLICE_SHIRO_URI=$STATIC_BASE_ARTIFACT_URI/${SPLICE_SHIRO_VERSION}.jar


RUN \
  mkdir -p $APP_HOME/bin && \
  mkdir -p $APP_HOME/jars && \
  mkdir -p $APP_HOME/conf && \
  mkdir -p $APP_LOG_DIR && \
  mkdir -p $APP_PID_DIR && \
  mkdir -p /target && \
  chmod +w /target

RUN ls /opt
RUN ls /opt/app
RUN ls $APP_HOME
# Copy Shiro for authentication
RUN  cd $APP_HOME/jars && \
   wget -q   $SHIRO_CORE_URI  && \
   wget -q   $SHIRO_WEB_URI   && \
   wget -q   $SPLICE_SHIRO_URI

COPY ./jars/*.jar $APP_HOME/jars/
COPY ./scripts/* /
COPY ./conf/ $APP_HOME/conf/

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

RUN ls ${APP_HOME}/bin

CMD ["/run-application.sh"]
