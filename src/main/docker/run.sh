#!/usr/bin/env sh

COMMAND="java \
	-Dcom.sun.management.jmxremote \
	-Dcom.sun.management.jmxremote.port=8005 \
	-Dcom.sun.management.jmxremote.authenticate=false \
	-Dcom.sun.management.jmxremote.ssl=false \
	-Dvertx.cacheDirBase=/app/prebid-server/data/.vertx \
	-XX:+UseParallelGC \
	-jar /app/prebid-server/prebid-server.jar"

echo $COMMAND
	
if [ "$VAULT_ADDR" = "" ]
then
   
   echo "Not using VAULT"
   		eval $COMMAND
   		
else

   echo "Using VAULT, securing rubicon.datasource.password"
	
	#Get vault token using role_id
	CLIENT_TOKEN=$(curl -sX POST -d "{\"role_id\":\"$ROLE_ID\"}" $VAULT_ADDR/v1/auth/approle/login  | jq ".auth.client_token")
	
	# Generate config
cat << EOF > config.hcl
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
	 
	/app/rp-envconsul/bin/envconsul -config="./config.hcl" $COMMAND
fi