#!/usr/bin/env python

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

data = pd.DataFrame([
    ["groupByKey", 6239],
    ["groupByKey", 5799],
    ["groupByKey", 5319],
    ["groupByKey", 6942],
    ["groupByKey", 6066],
    ["groupByKey-agg", 4392],
    ["groupByKey-agg", 4194],
    ["groupByKey-agg", 4775],
    ["groupByKey-agg", 4428],
    ["groupByKey-agg", 4162],
    ["reduceByKey", 3564],
    ["reduceByKey", 3728],
    ["reduceByKey", 3856],
    ["reduceByKey", 3411],
    ["reduceByKey", 3647],
 1
], columns=["implementation", "time (ms)"])

plt.figure(figsize=(6,4))
sns.barplot(data=data, x='implementation', y='time (ms)', capsize=.2)
plt.tight_layout()
plt.savefig("images/word-count-implementations.png")

print(data.groupby("implementation").mean())