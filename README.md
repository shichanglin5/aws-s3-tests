# aws_s3_tests
### 【SeaweedFS】Server Test Enviroment Setup
```go
# 1、配置 collection，集成测试会创建上千个 bucket，共用同一个 collection，避免本地创建大量 volume
fs.configure -locationPrefix / -replication 000 -volumeGrowthCount 2 -collection s3tests -apply

# 2、配置 admin account 的 accessKey 和 secretKey
s3.configure -apply -user admin -actions Read,Write,List,Tagging,Admin -access_key AKIARVTQO2DJNBALAG2Y -secret_key 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

3、启动 Seaweedfs Server，配置按需调整
weed server -config_dir=./ -v=0 -alsologtostderr=true -logdir=logs server -ip=127.0.0.1 -dir=volume -master.dir=master -master.volumeSizeLimitMB=300 -master.volumePreallocate=false -master=true -volume=true -filer=true -s3=true -s3.config=identity.json -iam=false -webdav=false -volume.max=10
```

### 【Aws-S3-Tests】Test Setup
```shell
git clone git@github.com:shichanglin5/aws_tests.git
# 提前安装好 python 环境（version 3.9)
cd aws_tests && pip3.9 install -r requirements.txt
mkdir -p .wd/xmind_exports
aws_config=config/config-seaweedfs.yaml python3.9 -m core.main
```

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

![demo.png](doc/demo.png)