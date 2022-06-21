import scipy.stats
import numpy as np

def t_statistic(x1, x2, s1, s2, n1, n2):

    sp = ((n1 - 1)*s1**2 + (n2 - 1)*s2**2) / (n1 + n2 - 2)
    t = (x1 - x2) / (sp * np.sqrt((s1 / n1) + (s2 / n2)))
    dof = n1 + n2 - 2

    # Determine the p-value
    p = scipy.stats.t.sf(abs(t), df=dof)

    print(f't-statistic: {t}')
    print(f'p-value: {p}')


