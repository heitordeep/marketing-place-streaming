#!/bin/bash

BUCKET_NAME=$1
ACTION=$2
PATH=$3

case $ACTION in 
    create) 
        /usr/local/bin/awslocal s3api create-bucket --bucket $BUCKET_NAME ;;
    move_obj) 
        /usr/local/bin/awslocal s3 mv /tmp/itau-shop/input/ s3://$BUCKET_NAME/input/ --recursive ;;
    list) 
        /usr/local/bin/awslocal s3 ls s3://$BUCKET_NAME/$PATH ;;
    all) 
        /usr/local/bin/awslocal s3 ls s3:// ;;
    *) 
        echo "$ACTION action not allowed" ;;
esac

if [ $? -ne 0 ] && [ $ACTION = "list" ]; then
    echo "Path does not exist"
fi

# exit $?