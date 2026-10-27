export default {
  config: {
    metrics: {
      entries: '条目',
      entriesMeta: '匹配当前筛选',
      groups: '分组',
      groupsMeta: '顶层前缀',
      settings: '配置项',
      settingsMeta: '已加载配置值',
      tunable: '可热改',
      tunableMeta: '无需重启即可修改'
    },
    title: '配置',
    subtitle: '当前为控制台提供服务的进程的实际生效配置',
    searchLabel: '搜索',
    searchPlaceholder: '按属性名搜索',
    groupLabel: '分组',
    allGroups: '全部分组（{count}）',
    groupOption: '{group}（{count}）',
    sourceLabel: '来源',
    allSources: '全部来源（{count}）',
    sourceOption: '{source}（{count}）',
    property: '属性',
    source: '来源',
    sources: {
      default: '默认值',
      file: '配置文件',
      env: '环境变量',
      cli: 'CLI',
      derived: '推导值',
      runtime: '运行时'
    },
    value: '值',
    actions: '操作',
    empty: '没有匹配的配置项。',
    node: '节点',
    tunableOnly: '只看可热改',
    editHint: '带铅笔图标的行可以在这里直接修改，保存后立即生效。',
    edit: '编辑',
    revert: '恢复为文件值',
    readonly: '只读',
    runtimeSaved: '{key} 已更新。',
    runtimeReverted: '{key} 已恢复为文件值。',
    runtimeFile: '文件值',
    legendTitle: '来源标签的含义',
    legendDerived:
      '推导值 —— 进程读完配置文件后又算了一遍得到的值，所以你在文件里写的那个数并不是生效值。master 端口拆分和主机名解析都属于这一类。',
    legendRuntime:
      '运行时 —— 有人在这个运行中的进程上覆盖了该值。它不会持久化，进程重启即失效，配置文件仍然是唯一权威的来源。',
    unresolvedTitle: '无法解析的运行时键',
    unresolvedBody:
      '这些键已注册，但对应的配置路径在发布的配置树中不存在，因此已在其它位置全部隐藏。后端与配置结构已经不一致。',
    volatileNote: '运行时覆盖不会持久化，进程重启后丢失。'
  }
}
