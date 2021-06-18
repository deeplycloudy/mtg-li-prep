import numpy as np
east_edge_glm = np.radians(15/2.0)*35785023.0
regions = {
#     'east_atlantic':(3.9e6, np.radians(15/2.0)*35785023.0, -6e5, 6e5),
    'east_atlantic_north':(east_edge_glm-20e5, east_edge_glm, -10e5, 10e5),
    'east_atlantic_south':(east_edge_glm-20e5+3e5, east_edge_glm, -27e5, -7e5),
#     'antilles':(7e5, 2e6, 1.3e6, 2.6e6),
#     'venezuela':(7e5, 2e6, 0.0e6, 1.3e6),
    'peru':(-7e5, 13e5, -16e5, 4e5),
    'amazon':(10e5, 30e5, -10e5, 10e5),
    #     'amazon':(7e5, 2e6, -1.3e6, 0.0e6),
    'subtropical_brazil':(1.3e6, 3.3e6, -2.7e6, -0.7e6),
    'south_atlantic':(2.7e6, 4.2e6, -4.0e6, -2.5e6),
#     'mid_atlantic':(2.3e6, 4.3e6, 2.0e6, 3.0e6),
    'mid_atlantic':(east_edge_glm-20e5, east_edge_glm, 10e5, 30e5),
}