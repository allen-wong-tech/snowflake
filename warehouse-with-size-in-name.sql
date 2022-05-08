/*
make warehouse with the size in the name which is helpful in SnowSight

*/
use role sysadmin;
create warehouse if not exists XS with warehouse_size = 'xsmall' auto_suspend = 1 initially_suspended = true;
create warehouse if not exists S with warehouse_size = 'small' auto_suspend = 1 initially_suspended = true;
create warehouse if not exists M with warehouse_size = 'medium' auto_suspend = 1 initially_suspended = true;
create warehouse if not exists L with warehouse_size = 'large' auto_suspend = 1 initially_suspended = true;
create warehouse if not exists XL with warehouse_size = 'xlarge' auto_suspend = 1 initially_suspended = true;
create warehouse if not exists XL2 with warehouse_size = 'x2large' auto_suspend = 1 initially_suspended = true;
create warehouse if not exists XL3 with warehouse_size = 'x3large' auto_suspend = 1 initially_suspended = true;
create warehouse if not exists XL4 with warehouse_size = 'x4large' auto_suspend = 1 initially_suspended = true;

/* XSMALL SMALL MEDIUM LARGE XLARGE X2LARGE X3LARGE X4LARGE  */
