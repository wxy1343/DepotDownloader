# steam仓库清单文件下载

## 全局参数

* `-t, --thread-num`: 线程数(默认32)
* `-o, --save-path`: 下载路径
* `-s, --server`: 指定cdn下载,可指定多个或`,`分隔
* `-l, --level`: 日志等级
* `-r, --retry-num`: 重试次数(默认3)

## 子命令参数

* `app`: 下载全部清单
    * `-p, --app-path`: 清单目录
        * [目录结构](https://github.com/wxy1343/ManifestAutoUpdate/tree/10)
            * `*.manifest`: 清单文件
            * `config.vdf`: 密钥文件
* `depot`: 单独下载清单
    * `-m, --manifest-path`: 清单文件路径,可指定多个或`空格`分隔
    * `-k, --depot-key`: 仓库密钥,可指定多个或`空格`分隔

## 使用示例

* `python main.py app --app-path ./10`
* `python main.py depot --manifest-path "368010_6622130648560741481.manifest" --depot-key ef8ea30154f995c4e4226df06f5cc39705ef0fc2d800f948613d1b3dd6b6437e`

## 下载加速

1. 指定cdn下载
    * 使用示例：`python main.py -s https://google.cdn.steampipe.steamcontent.com {app,depot} ...`
    * 指定多个用`,`分开,或者指定多个`-s`
    * cdn列表
        * google
            * `https://google.cdn.steampipe.steamcontent.com`
            * `https://google2.cdn.steampipe.steamcontent.com`
        * level3
            * `https://level3.cdn.steampipe.steamcontent.com`
        * akamai
            * `https://steampipe.akamaized.net`
            * `https://steampipe-kr.akamaized.net`
            * `https://steampipe-partner.akamaized.net`
        * 金山云
            * `http://dl.steam.clngaa.com`
        * 白山云
            * `http://st.dl.eccdnx.com`
            * `http://st.dl.bscstorage.net`
            * `http://trts.baishancdnx.cn`

2. 使用工具：[UsbEAm Hosts Editor](https://www.dogfight360.com/blog/475/)

## 旧清单导入steam运行

* steam导入旧清单无法下载
* 使用本工具下载旧清单文件到steam游戏目录
* 使用[steamtools](https://steamtools.net/)开启`阻止游戏下载与更新`，点击下载完空包即可游玩旧版本