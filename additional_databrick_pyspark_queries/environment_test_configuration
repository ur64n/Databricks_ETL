# Notebook 1 secret scope #

portfoliodb1_raw_sas_token = dbutils.secrets.get(scope = 'portfolio-scope', key = 'SASTokenRawContainer')

# Notebook 2 secrets configuration #

spark.conf.set("fs.azure.account.auth.type.portfoliodb1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.portfoliodb1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.portfoliodb1.dfs.core.windows.net", portfoliodb1_raw_sas_token)

# Notebook 3 test container connectivity #

display(dbutils.fs.ls("abfss://raw@portfoliodb1.dfs.core.windows.net/"))
