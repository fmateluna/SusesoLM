-- SQLBook: Code

-- SQLBook: Code
-- Endpoint: /lm/doctor/licenses
-- Listado de LM emitida por un médico
select
	l.id_lic ,
	l.operador,
	l.ccaf ,
	l.entidad_pagadora,
	l.folio ,
	l.fecha_emision ,
	l.empleador_adscrito,
	l.codigo_interno_prestador,
	l.comuna_prestador,
	l.fecha_ultimo_estado,
	l.ultimo_estado,
	l.rut_trabajador ,
	l.sexo_trabajador,
	l.edad_trabajador,
	l.tipo_reposo,
	l.dias_reposo,
	l.fecha_inicio_reposo,
	l.comuna_reposo,
	l.tipo_licencia,
	l.rut_medico,
	ep.descripcion_especialidad_profesional,
	pro.descripcion_profesionalidad,
	l.tipo_licencia_pronunciamiento,
	l.codigo_continuacion_pronunciamiento,
	l.dias_autorizados_pronunciamiento,
	l.codigo_diagnostico_pronunciamiento,
	l.codigo_autorizacion_pronunciamiento,
	l.causa_rechazo_pronunciamiento ,
	l.tipo_reposo_pronunciamiento,
	l.derecho_a_subsidio_pronunciamiento,
	l.rut_empleador,
	l.calidad_trabajador ,
	l.actividad_laboral_trabajador,
	l.ocupacion,
	l.entidad_pagadora_zona_c,
	l.fecha_recepcion_empleador,
	l.regimen_previsional,
	l.entidad_pagadora_subsidio,
	l.comuna_laboral,
	l.comuna_uso_compin,
	l.cantidad_de_pronunciamientos,
	l.cantidad_de_zonas_d,
	l.secuencia_estados ,
	l.cod_diagnostico_principal,
	l.cod_diagnostico_secundario,
	l.periodo ,
	ps.rn,
	ps.rn2,
	ps.frecuencia_mensual,
	ps.frecuencia_semanal,
	ps.otorgados_mensual,
	ps.otorgados_semanal,
	ps.ml,
	ps.score
from
	ml.licencias l,
	ml.especialidad_profesional_medicos epm ,
	ml.especialidad_profesional ep,
	ml.propensity_score ps,
	ml.profesionalidad pro,
	ml.profesionalidad_medicos prome
where
	 1=1
	 and l.folio = :folio
	 and epm.rut_medico = l.rut_medico 
	 and ep.id_especialidad_profesional = epm.id_especialidad_profesional
	 and ps.id_lic=l.id_lic
	 and pro.id_profesionalidad = prome.id_profesionalidad
	 and prome.rut_medico = l.rut_medico 
	