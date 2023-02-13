create table public.llamadas(
fecha           date,
rango_hora      varchar(10),
hora_valida     varchar(10),
campania        varchar(10),
direccion		varchar(20),
agente          varchar(50),
estado_llamada  varchar(50),
motivo          varchar(100),
submotivo       varchar(100),
tipo_de_emisor  varchar(50),
tipo_de_cliente varchar(50),
nivel_servicio  varchar(20),
aten_tmo        varchar(5),
wra_aten		int,
entrante        int,
atendida        int,
abandono        int,
tmo             int,
tme             int
);

-- 
--EXTRACT(MONTH FROM TIMESTAMP '2016-12-31 13:30:15')

-- delete from public.llamadas where EXTRACT(YEAR FROM fecha) = 2023 and EXTRACT(MONTH FROM fecha) = 02
select count(1) from public.llamadas where EXTRACT(YEAR FROM fecha) = 2023 and EXTRACT(MONTH FROM fecha) = 02 and EXTRACT(DAY FROM fecha) = 12
select * from public.llamadas where EXTRACT(YEAR FROM fecha) = 2023 and EXTRACT(MONTH FROM fecha) = 02 and EXTRACT(DAY FROM fecha) = 12

-- 1015
--
--delete from public.llamadas where fecha = current_date

select * from public.llamadas where fecha = current_date
--select  current_date

select EXTRACT(YEAR FROM fecha) as anio,EXTRACT(MONTH FROM fecha) as mes, count(1) as registros
from public.llamadas
group by EXTRACT(YEAR FROM fecha),EXTRACT(MONTH FROM fecha)
order by 1,2

CREATE INDEX index_fecha_campania
ON public.llamadas (fecha, campania);

select * from public.llamadas limit 50;
select count(1) from public.llamadas;
truncate table public.llamadas;
drop table public.llamadas;


select * from public.llamadas;

-- TEST
INSERT INTO public.llamadas
(fecha, rango_hora, hora_valida, campania, direccion, agente, estado_llamada, motivo, submotivo, tipo_de_emisor, tipo_de_cliente, nivel_servicio, aten_tmo, wra_aten, entrante, atendida, abandono, tmo, tme)
VALUES('2023-02-12', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 1, 1, 1, 1, 1, 1);


