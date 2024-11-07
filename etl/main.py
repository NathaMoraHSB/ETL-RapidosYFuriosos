import dim_cliente
import dim_fecha
import dim_hora
import dim_mensajero
import dim_novedad
import dim_sede
import trans_novedades
import trans_servicios
import hecho_novedades
import hecho_servicios_acumulating
import hecho_servicios_dia
import hecho_servicios_hora


# Create dimensiones
def create_dimensiones():
    print("Creating dimensiones")
    print("Creating dimension cliente")
    dim_cliente.create_dim_cliente()
    print("Finished creating dimension cliente")
    print("Creating dimension fecha")
    dim_fecha.create_dim_fecha()
    print("Finished creating dimension fecha")
    print("Creating dimension hora")
    dim_hora.create_dim_hora()
    print("Finished creating dimension hora")
    print("Creating dimension mensajero")
    dim_mensajero.create_dim_mensajero()
    print("Finished creating dimension mensajero")
    dim_novedad.create_dim_novedad()
    print("Finished creating dimension novedad")
    print("Creating dimension sede")
    dim_sede.create_dim_sede()
    print("Finished creating dimension sede")
    print("Finished creating dimensiones")


def create_transformaciones():
    print("Creating transformations")
    print("Creating transformacion novedades")
    trans_novedades.create_trans_novedades()
    print("Finished creating transformacion novedades")
    print("Creating transformacion servicios")
    trans_servicios.create_trans_servicios()
    print("Finished creating transformacion servicios")
    print("Finished creating transformations")


def create_hechos():
    print("Creating hechos")
    print("Creating hecho novedades")
    hecho_novedades.create_hecho_novedades()
    print("Finished creating hecho novedades")
    print("Creating hecho servicios acumulating")
    hecho_servicios_acumulating.create_hecho_servicios_acumulating()
    print("Finished creating hecho servicios acumulating")
    print("Creating hecho servicios dia")
    hecho_servicios_dia.create_hecho_servicios_dia()
    print("Finished creating hecho servicios dia")
    print("Creating hecho servicios hora")
    hecho_servicios_hora.create_hecho_servicios_hora()
    print("Finished creating hecho servicios hora")
    print("Finished creating hechos")


if __name__ == '__main__':
    create_dimensiones()
    create_transformaciones()
    create_hechos()
