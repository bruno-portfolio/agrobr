Output docs/assets/demo.gif

Set Shell "cmd"
Set FontSize 16
Set Width 1000
Set Height 600
Set Theme "Dracula"
Set Padding 20
Set TypingSpeed 50ms

Type "python"
Enter
Sleep 1s

Type "from agrobr.sync import cepea"
Enter
Sleep 1s

Type "df = cepea.indicador('soja')"
Enter
Sleep 8s

Type "df.head(10)"
Enter
Sleep 3s

Type "print(f'Total: {len(df)} registros')"
Enter
Sleep 3s
