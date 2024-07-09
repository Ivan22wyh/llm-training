import subprocess

# 定义一个函数来运行系统命令
def run_command(command):
    try:
        subprocess.run(command, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"命令执行失败: {e}")

# 备份 sources.list 文件
#run_command("sudo mv /etc/apt/sources.list /etc/apt/sources.bak1")

# 创建新的 sources.list 文件并写入内容
sources_list_content = """
# 默认注释了源码镜像以提高 apt update 速度，如有需要可自行取消注释
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal main restricted universe multiverse
# deb-src https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal main restricted universe multiverse
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-updates main restricted universe multiverse
# deb-src https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-updates main restricted universe multiverse
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-backports main restricted universe multiverse
# deb-src https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-backports main restricted universe multiverse

deb http://security.ubuntu.com/ubuntu/ focal-security main restricted universe multiverse
# deb-src http://security.ubuntu.com/ubuntu/ focal-security main restricted universe multiverse

# 预发布软件源，不建议启用
# deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-proposed main restricted universe multiverse
# # deb-src https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-proposed main restricted universe multiverse
"""

with open("/etc/apt/sources.list", "w") as f:
    f.write(sources_list_content)

# 更新和升级系统
run_command("sudo apt-get update")
run_command("sudo apt-get upgrade -y")

# 安装 zsh
run_command("sudo apt install zsh -y")

# 安装必备软件包
run_command("sudo apt-get -y install build-essential nghttp2 libnghttp2-dev libssl-dev")

# 安装 oh-my-zsh
run_command('sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"')

# 克隆 zsh 插件
run_command("git clone https://github.com/zsh-users/zsh-autosuggestions $ZSH_CUSTOM/plugins/zsh-autosuggestions")
run_command("git clone https://github.com/zsh-users/zsh-syntax-highlighting.git $ZSH_CUSTOM/plugins/zsh-syntax-highlighting")

# 复制 .zshrc 文件
run_command("cp /mnt/geogpt-gpfs/llm-course/home/wenyh/.zshrc ~/.zshrc")

# 加载 .zshrc
run_command("source ~/.zshrc")
