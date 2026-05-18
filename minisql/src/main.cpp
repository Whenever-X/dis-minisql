#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"
#include "common/config.h"

extern "C" {
int yyparse(void);
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
  // LOG(INFO) << "glog started!";
}

void InputCommand(char *input, const int len) {
  memset(input, 0, len);
  printf("minisql > ");
//  fflush(stdout);
  int i = 0;
  char ch;
  while ((ch = getchar()) != ';') {
    input[i++] = ch;
  }
  input[i] = ch;  // ;
  getchar();      // remove enter
}

std::vector<std::string> LoadBatchCommands(const char *path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    printf("failed to open batch file: %s\n", path);
    exit(1);
  }
  std::stringstream buffer;
  buffer << input.rdbuf();
  std::string all = buffer.str();
  std::vector<std::string> commands;
  std::string current;
  for (char ch : all) {
    current.push_back(ch);
    if (ch == ';') {
      commands.push_back(current);
      current.clear();
    }
  }
  if (!current.empty() && current.find_first_not_of(" \t\r\n") != std::string::npos) {
    current.push_back(';');
    commands.push_back(current);
  }
  return commands;
}

dberr_t ExecuteCommand(const std::string &command, ExecuteEngine &engine,
                       TreeFileManagers &syntax_tree_file_mgr, uint32_t &syntax_tree_id) {
  YY_BUFFER_STATE bp = yy_scan_string(command.c_str());
  if (bp == nullptr) {
    LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
    exit(1);
  }
  yy_switch_to_buffer(bp);

  MinisqlParserInit();
  yyparse();

  if (MinisqlParserGetError()) {
    printf("%s\n", MinisqlParserGetErrorMessage());
  } else {
    #ifdef ENABLE_SYNTAX_DEBUG
    printf("[INFO] Sql syntax parse ok!\n");
    SyntaxTreePrinter printer(MinisqlGetParserRootNode());
    printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
    #endif
  }

  auto result = engine.Execute(MinisqlGetParserRootNode());

  MinisqlParserFinish();
  yy_delete_buffer(bp);
  yylex_destroy();

  engine.ExecuteInformation(result);
  return result;
}

int main(int argc, char **argv) {
  setbuf(stdout,NULL);
  InitGoogleLog(argv[0]);
  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];
  // executor engine
  ExecuteEngine engine;
  // for print syntax tree
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  uint32_t syntax_tree_id = 0;

  if (argc == 3 && std::string(argv[1]) == "--batch") {
    for (const auto &command : LoadBatchCommands(argv[2])) {
      auto result = ExecuteCommand(command, engine, syntax_tree_file_mgr, syntax_tree_id);
      if (result == DB_QUIT) {
        break;
      }
    }
    return 0;
  }

  if (argc == 2 && std::string(argv[1]) == "--repl") {
    std::string command;
    while (std::getline(std::cin, command)) {
      if (command.empty()) {
        continue;
      }
      if (command.back() != ';') {
        command.push_back(';');
      }
      auto result = ExecuteCommand(command, engine, syntax_tree_file_mgr, syntax_tree_id);
      printf("\n__MINISQL_END__\n");
      fflush(stdout);
      if (result == DB_QUIT) {
        break;
      }
    }
    return 0;
  }

  while (1) {
    // read from buffer
    InputCommand(cmd, buf_size);
    auto result = ExecuteCommand(cmd, engine, syntax_tree_file_mgr, syntax_tree_id);

    // quit condition
    if (result == DB_QUIT) {
      break;
    }
  }
  return 0;
}