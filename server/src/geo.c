#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>
#include <math.h>
#include <pthread.h>
#include "bucket.h"
#include "table.h"
#include "geo.h"
#include "utils.h"
#include "server.h"
#include "crabql.h"
#include "slab.h"

#define EARTH_RADIUS 6378137
#define EARTH_CIRCUMFERENCE 40075017

struct polygon *polygon_make(struct point *vertexes, uint32_t num_vertexes) {
	struct polygon *polygon = slab_alloc(sizeof(struct polygon) + num_vertexes * sizeof(vertexes[0]));
	polygon->offlat = (uint32_t) (-1);
	polygon->maxlat = 0;
	polygon->offlon = (uint32_t) (-1);
	polygon->maxlon = 0;

	for (uint32_t i = 0; i < num_vertexes; i++) {
		uint32_t intlat = vertexes[i].lat;
		uint32_t intlon = vertexes[i].lon;

		if (intlat < polygon->offlat)
			polygon->offlat = intlat;
		if (intlat > polygon->maxlat)
			polygon->maxlat = intlat;
		if (intlon < polygon->offlon)
			polygon->offlon = intlon;
		if (intlon > polygon->maxlon)
			polygon->maxlon = intlon;
	}

	polygon ->num_vertexes = num_vertexes;
	for (uint32_t i = 0; i < num_vertexes; i++) {
		polygon->vertexes[i].lat = vertexes[i].lat - polygon->offlat;
		polygon->vertexes[i].lon = vertexes[i].lon - polygon->offlon;
	}
	polygon->maxlat -= polygon->offlat;
	polygon->maxlon -= polygon->offlon;
	return polygon;
}

size_t polygon_size(struct polygon *polygon) {
	return sizeof(struct polygon) + polygon->num_vertexes * sizeof(polygon->vertexes[0]);
}

struct circle *circle_make(struct point center, uint32_t radius) {
	struct circle *circle = slab_alloc(sizeof(struct circle));
	circle->center = center;
	if (radius > EARTH_CIRCUMFERENCE / 2)
		radius = EARTH_CIRCUMFERENCE / 2;
	circle->radius = radius;

	// TODO: calculate minlat, maxlat, minlon, maxlon....
	int32_t lat_delta_per_meter = 214; // > (1 << 32) / (EARTH_CIRCUMFERENCE / 2)
	//lon_delta_per_meter = ///////

	int32_t lat_delta = lat_delta_per_meter * radius;

	circle->minlat = center.lat - lat_delta;
	if (circle->minlat > center.lat)
		circle->minlat = 0;

	circle->maxlat = center.lat + lat_delta;
	if (circle->maxlat < center.lat)
		circle->maxlat = (uint32_t) -1;

	circle->minlon = 0;
	circle->maxlon = (uint32_t) -1;

	return circle;
}

double angle2d(struct point p, struct point p1, struct point p2) { 
	double dlat1 = (double) p1.lat - (double) p.lat;
	double dlon1 = (double) p1.lon - (double) p.lon;
	double dlat2 = (double) p2.lat - (double) p.lat;
	double dlon2 = (double) p2.lon - (double) p.lon;

    double theta1 = atan2(dlon1, dlat1);
    double theta2 = atan2(dlon2, dlat2);
    double dtheta = theta2 - theta1;
    while (dtheta > M_PI)
        dtheta -= M_PI * 2;
    while (dtheta < -M_PI)
        dtheta += M_PI * 2;
    return dtheta;
}

struct point point_make(uint32_t lat, uint32_t lon) {
	struct point point = {
		.lat = lat,
		.lon = lon,
	};
	return point;
}

static uint8_t point_in_polygon(struct point point, struct polygon *polygon) {
	point.lat -= polygon->offlat;
	point.lon -= polygon->offlon;
	if (point.lon > polygon->maxlon || point.lat > polygon->maxlat)
		return 0;

	double angle = 0;

	for (uint32_t i = 1; i < polygon->num_vertexes; i++)
		angle += angle2d(point, polygon->vertexes[i - 1], polygon->vertexes[i]);
	angle += angle2d(point, polygon->vertexes[polygon->num_vertexes - 1], polygon->vertexes[0]);
	return fabs(fabs(angle) - M_PI * 2) < 0.01;
}

static uint8_t point_in_circle(struct point point, struct circle *circle) {
	if (point.lat < circle->minlat || point.lat > circle->maxlat)
		return 0;
	
	/* TODO: compare lon */

	return geo_dist(*(uint64_t *) &point, *(uint64_t *) &circle->center) <= circle->radius;
}

#define SCALE 1000

static double sindeg_table[360 * SCALE + 1] = {0};

void sindeg_table_init() {
	for (int i = 0; i < 360 * SCALE + 1; i++) {
		double x = i * M_PI / 180 / SCALE;
		//double x2 = (i + 1) * M_PI / 180 / SCALE;
		sindeg_table[i] = sin(x);
	}
}

static double fastsindeg(double x) {
	return sin(x * M_PI / 180);
	int intx = x * SCALE;
	intx = (intx + 360 * SCALE * 100) % (360 * SCALE);
	if (intx < 5 * SCALE)
		return x * M_PI / 180;
	return sindeg_table[intx];
}

static double fastcosdeg(double x) {
	return fastsindeg(x + 90);
}

const double MAXVAL = 1 + (double) (uint32_t) (-1);

uint64_t geo_from_latlon(double lat, double lon) {
	if (lat < -90)
		lat = -90;
	if (lat > 90)
		lat = 90;
	
	uint32_t intlat = (uint32_t) ((90 - lat) / 180.0 * MAXVAL);
	uint32_t intlon = (uint32_t) ((180 + lon) / 360.0 * MAXVAL);

	uint64_t geo = (((uint64_t) intlon) << 32) | intlat;
	return geo;
}

static double lat_from_intlat(uint32_t intlat) {
	return 90 - intlat * 180.0 / MAXVAL;
}

static double lon_from_intlon(uint32_t intlon) {
	return intlon * 360.0 / MAXVAL - 180;
}

static void geo_to_latlon(uint64_t geo, double *lat, double *lon) {
	*lat = lat_from_intlat((uint32_t) geo);
	*lon = lon_from_intlon(geo >> 32);
}

static double get_dist(double lat1, double lon1, double lat2, double lon2) {
    double a = lat1 - lat2;
    double b = lon1 - lon2;

    double sinhalfa = fastsindeg(a / 2);
    double sinhalfb = fastsindeg(b / 2);

    double dist = 2 * asin(sqrt(sinhalfa * sinhalfa + fastcosdeg(lat1) * fastcosdeg(lat2) * sinhalfb * sinhalfb)) * 6378137;

    // uint32_t di = dist;
    // if (di < 20)
	   //  printf("dist = %"PRIu32", %lf, %lf %lf %lf %lf\n", di, dist, lat1, lon1, lat2, lon2);
    return dist;
}

uint32_t geo_dist(uint64_t geo1, uint64_t geo2)
{
	// TODO: use a LRU hash cache
	static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
	static uint64_t last_geo1, last_geo2;
	static uint32_t last_dist;
	uint32_t dist;
	pthread_mutex_lock(&mutex);
	if (last_geo1 == geo1 && last_geo2 == geo2) {
		dist = last_dist;
		pthread_mutex_unlock(&mutex);
		return dist;
	}
	pthread_mutex_unlock(&mutex);

	double lat1, lon1, lat2, lon2;
	geo_to_latlon(geo1, &lat1, &lon1);
	geo_to_latlon(geo2, &lat2, &lon2);

	dist = get_dist(lat1, lon1, lat2, lon2);

	pthread_mutex_lock(&mutex);
	last_geo1 = geo1;
	last_geo2 = geo2;
	last_dist = dist;
	pthread_mutex_unlock(&mutex);

	return dist;
}

int64_t cb_point_in_polygon(uint64_t point, size_t polygon) {
	return point_in_polygon(*(struct point *) &point, (struct polygon *) polygon) ? -1 : 0;
}

int64_t cb_point_in_circle(uint64_t point, size_t circle) {
	return point_in_circle(*(struct point *) &point, (struct circle *) circle) ? -1 : 0;
}

struct geo_result {
	uint32_t dist;
	char data[];
};

int command_nearby(struct request *request, msgpack_object o, msgpack_packer* response) {
	(void) request;
	(void) response;

	const char *geo_field_name = "geo";

    char bucket_name[64] = {0};
    msgpack_object *query_o = NULL;

    uint64_t center = 0;
    uint32_t maxdist = 200;

    if (o.type == MSGPACK_OBJECT_MAP) {
        msgpack_object_kv* p;
        for (p = o.via.map.ptr; p < o.via.map.ptr + o.via.map.size; p++) {
            if (mp_raw_eq(p->key, "bucket"))
                mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
            else if (mp_raw_eq(p->key, "query"))
                query_o = &p->val;
            else if (mp_raw_eq(p->key, "center"))
            	center = 333;
            else if (mp_raw_eq(p->key, "maxdist"))
            	maxdist = 333333;
        }
    }

    uint32_t limit = 20;
    
    struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);

    if (bucket == NULL)
    	goto done;

    pthread_rwlock_rdlock(&bucket->rwlock);
    struct schema *schema = bucket->schema;

    struct field *geo_field = schema_field_get(schema, geo_field_name);

   	const int SHIFT = 16;
    uint32_t table_id_middle = ((struct point *) &center)->lat >> SHIFT;
    uint32_t maxdelta = (maxdist << SHIFT) / 20000000 + 1;

    size_t record_size = ceildiv(schema->nbits, 8);
    size_t geo_result_size = sizeof(struct geo_result) + record_size;
    void *results = slab_alloc(limit * geo_result_size);
    size_t num_results = 0;

    struct crabql query;
	crabql_init(&query, query_o, schema, DATA_MODE_READ);

    for (uint32_t delta = 0; delta <= maxdelta; delta++) {
        for (uint64_t table_id = table_id_middle - delta; table_id <= table_id_middle + delta; table_id++) {
            if (table_id < (1U << SHIFT)) {
            	struct table *table = bucket_get_table(bucket, table_id, CAN_RETURN_NULL);

            	if (table == NULL)
            		continue;

            	pthread_rwlock_rdlock(&table->rwlock);

            	for (uint32_t i = 0; i < table->len; i++) {
            		void *data = table->data + i * record_size;
            		uint64_t item_geo = field_data_get(geo_field, data);

            		uint32_t dist = geo_dist(center, item_geo);

            		if (dist <= maxdist) {
        				if (num_results < limit) {
	            			struct geo_result *result = results + num_results * geo_result_size;
	            			result->dist = dist;
	            			memcpy(result->data, data, record_size);
	            			num_results++;
	            		}
            		}
            	}

            	pthread_rwlock_unlock(&table->rwlock);
            }
        }
    }

    slab_free(results);

done:

    return 0;
}
