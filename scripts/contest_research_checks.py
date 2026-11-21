#!/usr/bin/env python3
"""Reproducible local official-data analyses, no paid API requests."""
import json
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backend'))
from marketlab.storage import Lab
from marketlab.research import create,execute
from marketlab.contracts import utcnow

l=Lab();out={'created_at':utcnow(),'items':[],'llm_calls':0}
for tool in ['quality_audit','event_evidence','feature_ablation','macro_lag_sensitivity','warning_sweep']:
    spec={'tool':tool,'hypothesis':{'quality_audit':'核对来源、缺口与可得性，缺失项不得伪造。','event_evidence':'复核2020年3月压力变化是否有跨资产支持，并保留事件后的反证。','feature_ablation':'在同样的行、时间窗和分类器下比较价格与条件性文本，检验是否存在可复现增益。','macro_lag_sensitivity':'检验匿名序列在7/30/60日假设滞后下的敏感性，不解释匿名指标身份。','warning_sweep':'在验证区间比较预警数量和冷却规则，不据测试区间调参。'}[tool],'day':'2020-03-16','model_ids':['f7817b51d0d3aa6d431a2adb']}
    rid=create(l,'acceptance-local',spec);l.execute("UPDATE research SET status='approved' WHERE id=?",(rid,))
    started=time.monotonic()
    try:
        r=execute(l,rid,'acceptance-local')
        out['items'].append({'id':rid,'tool':tool,'status':'review','seconds':time.monotonic()-started,'result':r['result']})
        print(tool,'completed',flush=True)
    except Exception as exc:
        out['items'].append({'id':rid,'tool':tool,'status':'failed','error':str(exc)})
        print(tool,'failed:',str(exc),flush=True)
path=Path('docs/contest/research-checks-20261106.json');path.write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
print('Saved',path)
