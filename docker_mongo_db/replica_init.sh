#!/bin/bash

# Initiate replica set
echo "Initiating Replica set $MONGO_REPLICA_NAME"

mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --eval "
    rs.initiate({
        _id: '$MONGO_REPLICA_NAME',
        members: [
            { _id: 0, host: '$MONGO_SERVER_NAME'}
        ]
    });
"

# Check if the command succeeded
if [ $? -ne 0 ]; then
  echo "Replica set initiation failed."
  echo "Error occurred during MongoDB replica set initiation."
  exit 1
fi

echo "Replica set $MONGO_REPLICA_NAME initiated successfully!"
