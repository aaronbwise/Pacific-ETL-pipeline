from etl.etl import PacificEngine

# Value to switch between development (DEV) and production (PROD) databases
ENV = 'PROD'

def run_engine():
    obj = PacificEngine(ENV)
    obj.execute()


if __name__ == "__main__":
    run_engine()
