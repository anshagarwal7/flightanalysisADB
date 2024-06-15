# Databricks notebook source
containerName=dbutils.secrets.get(scope="training-secret",key="containername")
storageAccountName=dbutils.secrets.get(scope="training-secret",key="storageaccountname")
sas=dbutils.secrets.get(scope="training-secret",key="sas")
config= "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

#dbutils.fs.unmount('/mnt/source_blob/')

dbutils.fs.mount(
  source=dbutils.secrets.get(scope="training-secret",key="blob-mnt-path"),
  mount_point="/mnt/source_blob/",
  extra_configs={config:sas}
)

# COMMAND ----------

configs={
    "fs.azure.account.auth.type":"OAuth",
    "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id":dbutils.secrets.get(scope="training-secret",key="data-app-id"),
    "fs.azure.account.oauth2.client.secret":dbutils.secrets.get(scope="training-secret",key="data-app-secret"),
    "fs.azure.account.oauth2.client.endpoint":dbutils.secrets.get(scope="training-secret",key="data-client-refresh-url")
}



mountPoint="/mnt/raw_datalake/"
#dbutils.fs.unmount(mountPoint)

if not any(mount.mountPoint==mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=dbutils.secrets.get(scope="training-secret",key="datalake-raw"),
        mount_point=mountPoint,
        extra_configs=configs
    )

mountPoint="/mnt/cleansed_datalake/"
#dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint==mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=dbutils.secrets.get(scope="training-secret",key="datalake-cleansed"),
        mount_point=mountPoint,
        extra_configs=configs
    )


# COMMAND ----------

dbutils.fs.mounts()
