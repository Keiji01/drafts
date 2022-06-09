import json
import datetime


def fix_date_string_to_date(date):
    if isinstance(date, str):
        return datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")


def fixdatetimeobj_to_string_w_time(date):
    if isinstance(date, datetime.datetime):
        return datetime.datetime.strftime(date, "%Y-%m-%d %H:%M:%S")
    else:
        return date

def test():
    # Define o caminho e o inicio da task
    filein = "test.json"
    start_date = datetime.datetime.now()
    first_run = False

    try:
        # Abre arquivo json
        with open(filein, 'r') as read_file:
            read = read_file.read()
            if read:
                json_file_data = json.loads(read)
            else:
                # Se nunca rodou o incremental, vamos rodar os ultimos 10 minutos
                json_file_data = []
                dt = fixdatetimeobj_to_string_w_time(start_date - datetime.timedelta(minutes=10))
                first_run = True
            read_file.close()

        # Salva a data de inicio da task no json
        new_data = {
            "start_date": fixdatetimeobj_to_string_w_time(start_date),
            "total_updated_carts": "",
            "end_date": ""
        }
        json_file_data.append(new_data)

        # Salva o novo json com a data de início
        with open(filein, 'w') as write_file:
            write_file.write(json.dumps(json_file_data))
            write_file.close()

        if first_run:
            print("##### aqui mandar bala na execução da query #####")
            return

        else:
            # Ordena por start_date
            sorted_tasks = sorted(json_file_data, key=lambda d: d["start_date"])

            # Retira os que não concluiram a run completa
            completed_tasks = [x for x in sorted_tasks if x.get("end_date")]

            # Se não tiver concluido nenhuma run nunca, executaremos os ultimos 10 minutos
            if not completed_tasks[-1]:
                dt = fixdatetimeobj_to_string_w_time(start_date - datetime.timedelta(minutes=10))
            else:
                dt = completed_tasks[-1]["start_date"]
                print("O incremental começará em:" + dt)

            print("##### aqui mandar bala na execução da query #####")



        # Ao final da Run, precisamos salvar a end_date
        if True:
            # Salva a data de termino da task no json
            end_date = datetime.datetime.now()

            new_data = {
                "start_date": fixdatetimeobj_to_string_w_time(start_date),
                "total_updated_carts": "99",
                "end_date": fixdatetimeobj_to_string_w_time(end_date)
            }
            completed_tasks[-1].update(new_data)

            # Salva o novo json
            with open(filein, 'w') as write_file:
                write_file.write(json.dumps([completed_tasks[-1]]))
                write_file.close()

    except Exception as e:
        print(e)

test()