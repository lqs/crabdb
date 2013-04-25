#ifndef _GEO_H
#define _GEO_H

#include <stdint.h>

struct point {
	uint32_t lat;
	uint32_t lon;
};

#define point_to_uint64(point) (*(uint64_t *) &(point))

struct polygon {
	uint32_t offlat;
	uint32_t offlon;
	uint32_t maxlat;
	uint32_t maxlon;
	uint32_t num_vertexes;
	struct point vertexes[];
};

struct circle {
	struct point center;
	uint32_t radius;
	uint32_t minlat;
	uint32_t maxlat;
	uint32_t minlon;
	uint32_t maxlon;
};

void sindeg_table_init();
uint64_t geo_from_latlon(double lat, double lon);
uint32_t geo_dist(uint64_t geo1, uint64_t geo2);
struct polygon *polygon_make(struct point *vertexes, uint32_t num_vertexes);
size_t polygon_size(struct polygon *polygon);
struct circle *circle_make(struct point center, uint32_t radius);
int64_t cb_point_in_polygon(uint64_t point, size_t polygon);
int64_t cb_point_in_circle(uint64_t point, size_t circle);

#endif
