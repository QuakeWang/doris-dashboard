# Explain Outline Focus View Notes

## 背景
Explain 的文本信息密度高，Outline 视图容易出现信息冗余，影响快速定位分区剪裁、谓词下推、运行时过滤等关键信号。

## 目标
- 保留可视化的结构与关键信号。
- 减少重复信息与噪声节点。
- 保证在需要时仍可切回完整信息。

## 当前改动
- 新增 Focus signals 开关：仅展示关键诊断字段。
- 新增 Hide exchange/sink 开关：隐藏 EXCHANGE/SINK 节点并调整层级深度。
- Fragment 行提升为摘要卡片：展示 PARTITION/COLO/OUTPUT。
- Focus 模式下移除 Param 与 Signals 原始块，避免重复。
- Focus 模式下详情展开时隐藏摘要行，减少重复信息。

## 关键信号范围（Focus 模式）
- PREDICATES
- partitions/tablets
- PREAGGREGATION
- afterFilter
- cardinality
- runtime filters
- pushAggOp
- PARTITION / HAS_COLO_PLAN_NODE / OUTPUT EXPRS

## 使用方式
1) 打开 Outline。
2) 保持 Focus signals 开启，减少噪声。
3) 需要查看数据流时关闭 Hide exchange/sink。
4) 详情展开后只保留结构化字段，避免摘要重复。

## 相关代码
- apps/web/src/components/ExplainOutlineTree.tsx
- apps/web/src/styles.css

## 已知限制 / 待办
- Diagram 视图尚未同步 Focus/隐藏交换节点策略。
- 尚无搜索/高亮/Next/Prev 导航。
- 尚无“仅 Scan 节点”过滤。
