# point_in_time_grid

Point-in-time grid-based recipe for Pollination.

Use this recipe to calculate illuminance, irradiance, luminance or radiance for
a single point in time, given a HBJSON model and a sky condition.

Skies can be either CIE, ClimateBased/Custom, or for a specific
Illuminance/Irradiance. This input can also just be a text definition
of a sky's parameters. Examples include:

* cie 21 Mar 9:00 -lat 41.78 -lon -87.75 -tz 5 -type 0
* climate-based 21 Jun 12:00 -lat 41.78 -lon -87.75 -tz 5 -dni 800 -dhi 120
* irradiance 0
