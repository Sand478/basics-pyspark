# Установка Spark 3.2.3 на Ubuntu 22.04:


Обновляем системные пакеты. Займёт несколько минут.
```bash
sudo apt update && sudo apt -y full-upgrade
```

Для надёжности можно перезагрузить сервер:

```bash
sudo reboot now
```

Подождите пару минут и заходите на сервер снова.

Проверяем версию Java:
```bash
$ java -version
openjdk version "11.0.16" 2022-07-19
OpenJDK Runtime Environment (build 11.0.16+8-post-Ubuntu-0ubuntu122.04)
OpenJDK 64-Bit Server VM (build 11.0.16+8-post-Ubuntu-0ubuntu122.04, mixed mode, sharing)
```

Вероятно, Java не установлен. Устанавливаем:

```bash
sudo apt install openjdk-11-jre-headless
```

Проверяем снова версию Java:
```bash
$ java -version
openjdk version "11.0.17" 2022-10-18
OpenJDK Runtime Environment (build 11.0.17+8-post-Ubuntu-1ubuntu222.04)
OpenJDK 64-Bit Server VM (build 11.0.17+8-post-Ubuntu-1ubuntu222.04, mixed mode, sharing)
```

Качаем Spark:
```bash
cd /tmp
wget https://dlcdn.apache.org/spark/spark-3.2.3/spark-3.2.3-bin-hadoop3.2.tgz 
```

Экстрактим архив:
```bash
tar xvf spark-3.2.3-bin-hadoop3.2.tgz
```

Переносим в `/opt/`
```bash
sudo mv spark-3.2.3-bin-hadoop3.2 /opt/spark
```

Проверяем, что папка перенеслась:
```bash
$ ls -la /opt/spark
total 160
drwxr-xr-x 13 user user  4096 Nov 14 17:54 .
drwxr-xr-x  3 root      root       4096 Jan 12 08:13 ..
drwxr-xr-x  2 user user  4096 Nov 14 17:54 bin
drwxr-xr-x  2 user user  4096 Nov 14 17:54 conf
drwxr-xr-x  5 user user  4096 Nov 14 17:54 data
drwxr-xr-x  4 user user  4096 Nov 14 17:54 examples
drwxr-xr-x  2 user user 16384 Nov 14 17:54 jars
drwxr-xr-x  4 user user  4096 Nov 14 17:54 kubernetes
-rw-r--r--  1 user user 22878 Nov 14 17:54 LICENSE
drwxr-xr-x  2 user user  4096 Nov 14 17:54 licenses
-rw-r--r--  1 user user 57677 Nov 14 17:54 NOTICE
drwxr-xr-x  9 user user  4096 Nov 14 17:54 python
drwxr-xr-x  3 user user  4096 Nov 14 17:54 R
-rw-r--r--  1 user user  4512 Nov 14 17:54 README.md
-rw-r--r--  1 user user   167 Nov 14 17:54 RELEASE
drwxr-xr-x  2 user user  4096 Nov 14 17:54 sbin
drwxr-xr-x  2 user user  4096 Nov 14 17:54 yarn
```

Настраиваем переменные окружения, сначала забэкапив `~/.bashrc`:
```
sudo cp ~/.bashrc ~/.bashrc_bak
```

А затем открыв его:
```bash
vim ~/.bashrc
```
или
```bash
nano ~/.bashrc
```

Добавляем где-нибудь вверху
```
### SPARK SETTINGS

export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

### END: SPARK SETTINGS
```

Применяем изменения:
```bash
source ~/.bashrc
```

Проверяем, что изменения применились
```bash
$ echo $SPARK_HOME
/opt/spark
```

Пробуем запустить pyspark:
```bash
pyspark
```

Вы должны увидеть примерно следующее:
```
Python 3.10.6 (main, Nov 14 2022, 16:10:14) [GCC 11.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
23/01/12 08:16:15 WARN Utils: Your hostname, spark-server resolves to a loopback address: 127.0.1.1; using 10.0.0.31 instead (on interface eth0)
23/01/12 08:16:15 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/spark/jars/spark-unsafe_2.12-3.2.3.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
23/01/12 08:16:16 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.2.3
      /_/

Using Python version 3.10.6 (main, Nov 14 2022 16:10:14)
Spark context Web UI available at http://10.0.0.31:4040
Spark context available as 'sc' (master = local[*], app id = local-1673511378105).
SparkSession available as 'spark'.
>>> 
```

Выходим, введя:
```
exit()
```

## Проверим, может ли он исполнять скрипты

Создадим файл `/tmp/pyspark-test.py` через vim
```bash
vim /tmp/pyspark-test.py
```
или nano
```bash
nano /tmp/pyspark-test.py
```

И закинем в него тестовый скрипт:
```python
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import window


app_name = 'PySpark count distinct test'


# Создаём спарк контекст
spark = SparkSession.builder.appName(app_name).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

spark.sql('select 1').show()
```

И прокинем его в spark-submit:
```bash
spark-submit /tmp/pyspark-test.py
```

На выходе должны увидеть примерно следующее содержание:
```
23/01/12 08:17:47 WARN Utils: Your hostname, spark-server resolves to a loopback address: 127.0.1.1; using 10.0.0.31 instead (on interface eth0)
23/01/12 08:17:47 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/spark/jars/spark-unsafe_2.12-3.2.3.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
23/01/12 08:17:48 INFO SparkContext: Running Spark version 3.2.3
23/01/12 08:17:48 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
23/01/12 08:17:48 INFO ResourceUtils: ==============================================================
23/01/12 08:17:48 INFO ResourceUtils: No custom resources configured for spark.driver.
23/01/12 08:17:48 INFO ResourceUtils: ==============================================================
23/01/12 08:17:48 INFO SparkContext: Submitted application: PySpark count distinct test
23/01/12 08:17:48 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
23/01/12 08:17:48 INFO ResourceProfile: Limiting resource is cpu
23/01/12 08:17:48 INFO ResourceProfileManager: Added ResourceProfile id: 0
23/01/12 08:17:48 INFO SecurityManager: Changing view acls to: ulyumdzhi
23/01/12 08:17:48 INFO SecurityManager: Changing modify acls to: ulyumdzhi
23/01/12 08:17:48 INFO SecurityManager: Changing view acls groups to: 
23/01/12 08:17:48 INFO SecurityManager: Changing modify acls groups to: 
23/01/12 08:17:48 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(ulyumdzhi); groups with view permissions: Set(); users  with modify permissions: Set(ulyumdzhi); groups with modify permissions: Set()
23/01/12 08:17:49 INFO Utils: Successfully started service 'sparkDriver' on port 39167.
23/01/12 08:17:49 INFO SparkEnv: Registering MapOutputTracker
23/01/12 08:17:49 INFO SparkEnv: Registering BlockManagerMaster
23/01/12 08:17:49 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
23/01/12 08:17:49 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
23/01/12 08:17:49 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
23/01/12 08:17:49 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-b1efe5de-40f3-4958-9bce-9a5837fbd3d9
23/01/12 08:17:49 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
23/01/12 08:17:49 INFO SparkEnv: Registering OutputCommitCoordinator
23/01/12 08:17:49 INFO Utils: Successfully started service 'SparkUI' on port 4040.
23/01/12 08:17:49 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://10.0.0.31:4040
23/01/12 08:17:49 INFO Executor: Starting executor ID driver on host 10.0.0.31
23/01/12 08:17:49 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 46369.
23/01/12 08:17:49 INFO NettyBlockTransferService: Server created on 10.0.0.31:46369
23/01/12 08:17:49 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
23/01/12 08:17:49 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 10.0.0.31, 46369, None)
23/01/12 08:17:49 INFO BlockManagerMasterEndpoint: Registering block manager 10.0.0.31:46369 with 434.4 MiB RAM, BlockManagerId(driver, 10.0.0.31, 46369, None)
23/01/12 08:17:49 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 10.0.0.31, 46369, None)
23/01/12 08:17:49 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 10.0.0.31, 46369, None)
23/01/12 08:17:50 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
23/01/12 08:17:50 INFO SharedState: Warehouse path is 'file:/home/ulyumdzhi/spark-warehouse'.
+---+
|  1|
+---+
|  1|
+---+
```

## Установка Jupyter Notebook
Но для начала установим pip.
```bash
sudo apt install python3-pip
```

После чего установим jupyter
```bash
pip install jupyterlab
```

Перед запуском в терминале на **сервере** написать:
```
jupyter notebook password
```

Или попробуйте так, если не сработал код выше:
```
~/.local/bin/jupyter notebook password
```

Задать _пароль_. Подтвердить.

И в терминале **сервера** написать:
```
nohup ~/.local/bin/jupyter-lab
```

На своем **локальном компьютере** открываем терминал, пишем там команду, чтобы пробросить туннель по ssh к нашему серверу с запущенным Jupyter:
```bash
ssh -L 8000:localhost:8888 имя_админа_на_виртуальной_машине@ip-вашего-сервера
```
где `8888` - это порт jupyter на сервере по адресу `ip-вашего-сервера` к которому вы присвоили порт `8000` на вашем локальном компьютере.
Теперь возращайтесь в бразузер компьютера и открыть страницу в своем браузере:

```
http://localhost:8000/lab/
```
и вводите свой _пароль_ :)


<!-- Запуск ноутбука на сервере
```bash
nohup ~/.local/bin/jupyter-notebook
```

На своем **локальном компьютере** открываем терминал, пишем там команду, чтобы пробросить туннель по ssh к нашему серверу с запущенным Jupyter:
```bash
ssh -L 8000:localhost:8888 имя_админа_на_виртуальной_машине@внешний-ip-вашего-сервера
```
где `8888` - это порт jupyter на сервере по адресу `ip-вашего-сервера` к которому вы присвоили порт `8000` на вашем локальном компьютере.
Теперь возращайтесь в бразузер компьютера и обновляйте страницу с jupyter 

```
http://localhost:8000/lab/workspaces/auto-t
```
Должен открыть обычный интерфейс Jupyter. Это веб-интерфейс Jupyter открытый на вашем локальном компьютере, который соединен с сервером. -->


### Запускаем PySpark в Jupyter Notebook
Копируем в одну ячейку вашей тетрадки следующие команды:

```
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import window


app_name = 'PySpark test'


# Создаём спарк контекст
spark = SparkSession.builder.appName(app_name).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

spark.sql('select 1').show()
```

Если после запуска ячейки у вас вывелось:

```
+---+
|  1|
+---+
|  1|
+---+
```

То я вас поздравляю! Вы установили Spark на удаленном сервере и подключили к нему Jupyter со своего локального компьютера!


