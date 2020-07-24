#!/usr/bin/env sh

COMMAND="java \
	-Dcom.sun.management.jmxremote \
	-Dcom.sun.management.jmxremote.port=8005 \
	-Dcom.sun.management.jmxremote.authenticate=false \
	-Dcom.sun.management.jmxremote.ssl=false \
	-Dvertx.cacheDirBase=/app/prebid-server/data/.vertx \
	-XX:+UseParallelGC \
	-jar /app/prebid-server/prebid-server.jar"

read_host_id() {
  if [ "$HOST_ID" != "" ]; then
    echo "HOST_ID is already defined as $HOST_ID"
  elif [ "$ECS_CONTAINER_METADATA_URI" != "" ]; then
    echo "Looking for AWS ECS task id to set it as HOST_ID variable ..."

    task_id=$(curl -s "$ECS_CONTAINER_METADATA_URI/task" | jq ".TaskARN" | tr -d '"' | grep -Eo "[a-f0-9\-]+$")

    if [ "$task_id" != "" ]; then
      echo "Detected AWS ECS task id $task_id, setting it as HOST_ID variable"
      export HOST_ID="$task_id"
    else
      echo "AWS ECS task id could not be detected"
    fi
  else
    echo "HOST_ID and ECS_CONTAINER_METADATA_URI are not defined"
  fi

  if [ "$HOST_ID" = "" ]; then
    HOST_ID=$(cat /proc/sys/kernel/random/uuid)
    export HOST_ID

    echo "HOST_ID set to generated value $HOST_ID"
  fi
}

generate_vault_config() {
  # Get vault token using role_id
  CLIENT_TOKEN=$(curl -sX POST -d "{\"role_id\":\"$ROLE_ID\"}" $VAULT_ADDR/v1/auth/approle/login | jq ".auth.client_token")

  # Generate config
  cat <<EOF >config.hcl
vault {
  address = "$VAULT_ADDR"
  renew = true
  token = $CLIENT_TOKEN
}
secret {
  no_prefix = true
  path = "$SECRET_PATH"
}
EOF
}

# main

echo "Command to run: $COMMAND"

read_host_id

if [ "$VAULT_ADDR" = "" ]; then
  echo "Not using VAULT"

  eval $COMMAND
else
  echo "Using VAULT, securing sensitive configuration parameters"

  generate_vault_config

  /app/rp-envconsul/bin/envconsul -config="./config.hcl" $COMMAND
fi
