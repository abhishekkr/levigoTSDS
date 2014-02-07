# levigoTSDS

```ASCII

 ___                       ()    __,,,             ==||==  |====) ||=   |====)
  \ \    ___  __      __  ___    | _ \     ____      ||    \\     || @  \\
  | |   / ==\  \\    //    ||    \ " ,|,  / __ \     ||     +++   ||  @  +++
  | |  | ,--"   \\  //     ||   _/ _ \   | |  | |    ||       \\  ||  @     \\
  | |  | |_.    | \/ |     ||   |_ " /    \ \/ /     ||   /|__/ | || @  /|__/@
 _|_|_  \___\    \__/     _||_   ;;\/      \__/      ||   \____/  ||=   \____/

levigoTSDS ~ The same old leveldb with Time Series DataStore capabilities.

```

### Levigo Time Series Data Store

Suggested default key-type with Key as Parent NameSpace:
 "%KEY:%YEAR:%MONTH:%DAY:%HOUR:%SECOND" => VALUE

Also available Key Format is TimeStamp as Parent NameSpace:
 "%YEAR:%MONTH:%DAY:%HOUR:%SECOND:%KEY" => VALUE

Either of these shall be used seeing better suited to your usecase.
Both can be used together as well, but why?

[![baby-gopher](https://raw2.github.com/drnic/babygopher-site/gh-pages/images/babygopher-badge.png)](http://www.babygopher.org)
