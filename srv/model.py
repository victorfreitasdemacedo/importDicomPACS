from peewee import *

database = MySQLDatabase('import', host = '10.1.4.71', port = 3306, user = 'root', password = 'qwe456')

class estudo(database.Model):
  id = BigAutoField(primary_key=True)
  estudo = CharField()
  paciente = CharField()
  data_e_hora = DateTimeField()
  patientID = CharField()
  status = CharField(max_length=1)

class files(database.Model):
  id = BigAutoField(primary_key=True)
  sop = CharField()
  files = CharField()
  file_pacs = CharField()
  fk_id_estudo = ForeignKeyField(estudo, backref='id_estudo')

database.connect()

def createDatabase():
  database.create_tables([estudo, files])

def closeConexao():
  database.close()

def verificar_ou_inserir_estudo(std, pac, dth, pacid):
    try:
        rtd_estudo = estudo.get(estudo=std, paciente=pac, patientID=pacid)
    except estudo.DoesNotExist :
        rtd_estudo = estudo.create(estudo=std, paciente=pac, data_e_hora=dth, patientID=pacid, status='F' )
    return rtd_estudo

def verificar_ou_inserir_file(idsop, sfiles, dfiles, idestudo):
    try:
        files.get(files=sfiles, file_pacs=dfiles, fk_id_estudo=idestudo)
    except files.DoesNotExist :
        files.create(sop=idsop,files=sfiles,file_pacs=dfiles,fk_id_estudo=idestudo)

def verificar_validar(stat):
    return estudo.select().where(estudo.status == stat).order_by(estudo.id)

def verificar_validar_estudo(return_estudo):
    lista = []
    for j in files.select().where(files.fk_id_estudo == return_estudo):
       lista.append([j.sop, j.files, j.file_pacs])
    return lista

def update_tabela(upd):
    temp = estudo.update(status = upd["query"]).where(id == upd["id"])
    temp.execute()