### lianzhong spider

#### 缘由

大学时代很喜欢玩联众的四国军棋，闲暇之余也会去联众的四国bbs里面灌水，bbs里面人才辈出，妙文不断，令人佩服之极。
现今联众四国的游戏玩家非常少，今天偶然又逛了下bbs，发现几乎没人在上面灌水了，于是想把它备份下来。

此爬虫写得很糙，但可以爬取联众bbs的任何一个论坛版块，直接看代码修改下root_url即可。

与棋有声，落忆无痕

#### 用法

1. 安装依赖

    ```shell
    virtualenv .env
    .env/bin/pip install requirements.txt
    ```


2. 直接修改代码中修改mongodb入库地址

3. 运行
    ```shell
    nohup .env/bin/python bbs_spider.py > /dev/null 2>&1 &
    ```
    



