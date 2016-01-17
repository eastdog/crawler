__author__ = 'victor'

from math import sqrt, sin, atan2, cos


def bd_decrypt(bd):
    x_pi = 3.14159265358979324 * 3000.0 / 180.0
    x = bd[0] - 0.0065
    y = bd[1] - 0.006
    z = sqrt(x * x + y * y) - 0.00002 * sin(y * x_pi)
    theta = atan2(y, x) - 0.000003 * cos(x * x_pi)
    gg_lon = z * cos(theta)
    gg_lat = z * sin(theta)
    return gg_lon, gg_lat


if __name__ == '__main__':

    db = {'bd_lon': 121.5147324794, 'bd_lat': 31.2403629599}
    print bd_decrypt(db)