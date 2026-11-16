果品智能分选质量闭环平台

复检与出库：可通过 Application.receive_inspection 记录人工复检凭据，
包括果批、产地、分选线、相机批次、机判级别、复检级别、抽样缘由及时间；
同一凭据重放不重复入库，冲突凭据会被拒绝。Application.dispatch 只给已放行
果批分配一次出库目的地，优先采用最新人工复检，否则沿用机器判级，
已有出库分配不会被后续证据静默改写。记录可通过仓储的 inspection_rows
和 queue_rows 读取，均使用运行命令的同一数据库。

运行验收：
python3 -m orchard_grade run --fixture fixtures/acceptance.json --database /tmp/orchard_grade.sqlite3 --output /tmp/orchard_grade-output

相同批次再次运行（或 --output 指向新目录）时，不重新推进分选：交付由该次运行
结束时冻结的统计、批次状态与审计依据恢复，因此后来修订过的数据库内容不会被
当作当时结果。已存在的可信文件原样保留，缺失、损坏或只写出一部分的文件会重建
并在结果的 delivery 字段（trusted_files / created_files / repaired_files /
needs_reverification）中列明；恢复未完整核验前不会返回成功。两个班组同时请求
同一批次时，一方生成交付、另一方等待并恢复同一交付。冻结材料损坏或首次处理
中断时恢复失败，已生效的分选结论保持不变，并明确要求重新核验。首次交付时才
会推进分选；同一输出目录不会被另一次处理的交付覆盖。

核验：
python3 -m orchard_grade verify --database /tmp/orchard_grade.sqlite3 --output /tmp/orchard_grade-output
（同时核对审计链、目录文件哈希以及 delivery.run_id / delivery_hash 是否与
数据库中该次处理的冻结交付一致，重启后仍一致。）

全量测试：
python3 -m unittest discover -s tests -v

按模块测试：
python3 -m unittest tests.grading.test_decision_table -v

构建检查：
python3 -m compileall -q orchard_grade tests

inspection_work.open 手工登记复检工单，claim 领取，complete 在同一事务保存复检凭据并结束工单；果批资料更正后须重新确认，已完成工单不可重复派发。
