def get_stars(p_val: float):
    if p_val <= .01:
        return '***'
    elif p_val <= .05:
        return '**'
    elif p_val <= .1:
        return '*'
    else:
        return ''