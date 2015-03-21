import astevalscript

def main():
    script = astevalscript.Script()

    script.add_file("testasteval.py")

    script.invoke("testfunc", message="werkt dit?")

if __name__ == '__main__':
    main()
