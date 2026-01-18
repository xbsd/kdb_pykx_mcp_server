
/ Get texture size
/ Selects optimal POT (^2) texture dimensions to contain
/ at least num_elements (rectangular dimensions permitted)
get_texture_size:{[n]
  npow2:{[x]xexp[2;1+(floor xlog[2;x])]};
  width:npow2 sqrt n;
  r:1+floor xlog[2;width];
  height:xexp[2;min where (ts:width*xexp[2;] each til r)>=n];
  (width;height)
  };

point_to_gmaps:{[lat;lng]
  mercator_range:256;
  deg2rad:{x*((acos -1)%180)};
  pox:mercator_range%2;
  poy:mercator_range%2;
  ppld:mercator_range%360;
  pplr:mercator_range%(2*(acos -1));
  px:pox+(lng*ppld);
  siny:sin deg2rad lat;
  siny:?[siny<-0.9999;-0.9999;siny];
  siny:?[siny>0.9999;0.9999;siny];
  py:poy+(0.5*log (1+siny)%(1-siny))*neg pplr;
  (px;py)
  };

.luna.ts2epoch:{floor((`long$x)-`long$1970.01.01D00:00)%1e9}
.luna.epoch2ts:{`datetime$(z%x)-y}[prd 24 60 60 1000j;(2000.01.01-1970.01.01)]

.luna.table2layer_linesegment:{[df]
  / Translate lat,lng to projection
  proj:point_to_gmaps[df`lat;df`lng];
  df:update y:proj[1],x:proj[0] from df;

  / Calculate texture dimensions
  num_elements:count df;
  tex_wh:get_texture_size[num_elements];
  tex_w:tex_wh[0];
  tex_h:tex_wh[1];
  tex_size:`long$tex_w*tex_h;

  len_padding:`long$tex_size-(count df);
  padding:len_padding#0.0;

  / TBC - incomplete

  };

.luna.table2layer_points:{[df]
  / Translate lat,lng to projection
  proj:point_to_gmaps[df`lat;df`lng];
  df:update y:proj[1],x:proj[0] from df;

  / Derive rotational radians from heading (degrees)
  df:update r:heading*0.0174533 from df;

  / Calculate texture dimensions
  num_elements:count df;
  / tex_wh:get_texture_size[num_elements];
  / tex_w:tex_wh[0];
  / tex_h:tex_wh[1];
  / tex_size:`long$tex_w*tex_h;
  tex_w:0;
  tex_h:0;
  tex_size:`long$num_elements;

  / Calculate seconds offset for each snap_time
  df:update time_offset:(`long$(t)-(min t))%1e9 from `t xasc df;

  / Convert to fractional 0..1 ('t_basis')
  time_range:`long$((max df`t)-(min df`t))%1e9;
  df:update t_basis:time_offset%time_range from df;

  slices:asc distinct df`t_basis;
  len_padding:`long$tex_size-(count distinct df`id);
  padding:len_padding#0.0;

  / pivot
  / t_basis | id0 | id1 | id2 | ...

  P:asc exec distinct id from df;
  x_table:flip {reverse fills reverse fills x} peach flip ([]t_basis:distinct df`t_basis),'(flip (`${"pi",x} peach string P)!flip value exec value P#(id!x) by t_basis:t_basis from df);
  y_table:flip {reverse fills reverse fills x} peach flip ([]t_basis:distinct df`t_basis),'(flip (`${"pi",x} peach string P)!flip value exec value P#(id!y) by t_basis:t_basis from df);
  z_table:flip {reverse fills reverse fills x} peach flip ([]t_basis:distinct df`t_basis),'(flip (`${"pi",x} peach string P)!flip value exec value P#(id!r) by t_basis:t_basis from df);
  a_table:flip {reverse fills reverse fills x} peach flip ([]t_basis:distinct df`t_basis),'(flip (`${"pi",x} peach string P)!flip value exec value P#(id!spriteidx) by t_basis:t_basis from df);

  tx:flip value 1_flip x_table;
  ty:flip value 1_flip y_table;
  tz:flip value 1_flip z_table;
  ta:flip value 1_flip a_table;

  tx:{[x;p]x,p}[;padding] peach tx;
  ty:{[x;p]x,p}[;padding] peach ty;
  tz:{[x;p]x,p}[;padding] peach tz;
  ta:{[x;p]x,p}[;padding] peach ta;

  / ta:`float$(count tx;count first tx)#((count tx)*(count first tx))?10;
  / ta:tz;
  / ta:(count tx;count first tx)#1.0f;

  final:(0N;tex_size)#(raze tx),'(raze ty),'(raze tz),'(raze ta); / note changed from ta

  (`meta`data)!(`type`width`height`size`slices!(`raw;`long$tex_w;`long$tex_h;`long$tex_size;count final);([]id:til count final;t:.luna.ts2epoch distinct df`t;data:final))
  };

/ .luna.layer2disk[`$"walks/telewalk";layer]
.luna.layer2disk:{[prefix;layer]
  / save the .luna
  file:":",(string prefix),"_";
  if[(count layer)>9999;:`error_too_many_slices];
  {[x;m;f] (`$f,(string m`slices),"x",(string m`width),"x",(string m`height),"_",(ssr[-4$(),(string x`id);" ";"0"]),".luna") 1: raze {reverse 0x0 vs x} each `real$(raze/) x`data}[;layer`meta;file] peach layer`data
  };

.luna.data2blob:{[data]
  raze {reverse 0x0 vs x} each `real$(raze/) data
  };

.luna.layerts2blob:{[layer;ts]
  / render a single timeslice to a blob
  raze {reverse 0x0 vs x} each `real$(raze/) exec data from layer`data where id=ts
  };

