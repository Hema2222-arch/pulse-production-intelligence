import statistics
def ewma(values, alpha=0.2):
    x=values[0]
    for v in values[1:]: x=alpha*v+(1-alpha)*x
    return x
def z_score(baseline, value):
    mean=statistics.mean(baseline); sd=statistics.pstdev(baseline)
    return 0 if sd==0 else (value-mean)/sd
def test_spike():
    baseline=[45,48,51,47,52,49,50,46,53,48]
    assert z_score(baseline, 120) > 3
def test_ewma():
    assert ewma([10,10,10,30],.2) > 10
