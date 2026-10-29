# 集群 Web 截图

已在 WSL Ubuntu 上复用 `/home/u5566/hadoop-3.3.6`，以独立配置启动单机 HDFS/YARN 并完成全部六个 Job。截图取自真实浏览器页面。

1. `yarn-success.jpg`：ResourceManager，显示六个应用全部 FINISHED / SUCCEEDED，页面地址 `http://localhost:18088/cluster/apps`。
2. `jobhistory-success.jpg`：JobHistory，显示六个 Job 全部 SUCCEEDED，任务一至五训练各完成 5 个 Map，预测完成 4 个 Map、0 个 Reduce；任务三采用 1 个 Reduce。页面地址 `http://localhost:19888/jobhistory`。

作业 ID 为 `job_1793272345145_0001` 至 `job_1793272345145_0006`，运行日期为 2026-10-29（Asia/Shanghai）。服务停止后网页不再可访问，提交包内图片和 JSON 仍可离线核对。
